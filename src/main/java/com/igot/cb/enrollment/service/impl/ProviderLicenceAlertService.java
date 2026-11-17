package com.igot.cb.enrollment.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.igot.cb.producer.Producer;
import com.igot.cb.transactional.cassandrautils.CassandraOperation;
import com.igot.cb.util.CbServerProperties;
import com.igot.cb.util.Constants;
import com.igot.cb.util.cache.CacheService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.StringWriter;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Provider licence consumption alerts (SPV Licence Notifications). The thresholds are the
 * provider's saved alertthresholds that are switched on, or the configured defaults until it saves
 * its own; a threshold that is off is never marked, sent or re-armed. After an
 * enrolment consumes a provider licence, checkThresholds marks every threshold the consumption has
 * reached in Redis (licenceAlertFired_partnerId_threshold, set only if absent, no expiry) and
 * publishes one alert for the highest reached threshold when it is newly marked; a threshold added
 * below one already sent is only marked. The alert listener emails it to the provider's
 * recipients in BCC through the notification service. A threshold above the consumption is
 * re-armed (its key deleted), e.g. after the Over All Limit is raised.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class ProviderLicenceAlertService {

    private static final int EXHAUSTED_THRESHOLD = 100;
    private static final String IST_ZONE = "Asia/Kolkata";
    private static final DateTimeFormatter DATE_TIME_FORMAT = DateTimeFormatter.ofPattern("dd MMM yyyy, hh:mm a 'IST'", Locale.ENGLISH);

    private final CassandraOperation cassandraOperation;
    private final CacheService cacheService;
    private final CbServerProperties cbServerProperties;
    private final Producer producer;
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;

    /**
     * consumed: the provider's licences consumed after this enrolment. Never throws, so an alert
     * failure never fails the enrolment. Nothing is marked when alerts are off or there are no
     * recipients, and a failed publish clears the marks it set.
     */
    public void checkThresholds(String partnerId, JsonNode providerData, long consumed) {
        try {
            long overAllLimit = providerData.path(Constants.OVER_ALL_PROVIDER_LIMIT).asLong(0);
            if (overAllLimit <= 0) {
                return;
            }
            Map<String, Object> settings = readSettings(partnerId);
            List<Integer> reached = new ArrayList<>();
            for (int threshold : thresholdsOf(settings)) {
                if (consumed * 100 >= threshold * overAllLimit) {
                    reached.add(threshold);
                } else {
                    cacheService.deleteCache(alertKey(partnerId, threshold), cbServerProperties.getDefaultIndex());
                }
            }
            if (reached.isEmpty()) {
                return;
            }
            List<String> recipients = recipientsOf(settings);
            if (recipients.isEmpty()) {
                return;
            }
            String firedOn = Instant.now().toString();
            List<Integer> newlyReached = reached.stream()
                    .filter(threshold -> cacheService.setIfAbsent(alertKey(partnerId, threshold),
                            cbServerProperties.getDefaultIndex(), firedOn))
                    .toList();
            if (newlyReached.contains(Collections.max(reached))) {
                publishAlert(partnerId, providerData, consumed, overAllLimit, newlyReached, recipients);
            }
        } catch (Exception e) {
            log.error("Failed to check licence alert thresholds for partnerId: {}", partnerId, e);
        }
    }

    /**
     * Sends one alert event as an email to its recipients in BCC. On failure the threshold is
     * re-armed, so the next licence consumed sends it again.
     */
    @SuppressWarnings("unchecked")
    public void sendAlert(Map<String, Object> event) {
        String partnerId = (String) event.get(Constants.PARTNER_ID);
        int threshold = ((Number) event.get(Constants.THRESHOLD)).intValue();
        try {
            String partnerName = (String) event.get(Constants.PARTNER_NAME);
            long consumed = ((Number) event.get(Constants.CONSUMED)).longValue();
            long overAllLimit = ((Number) event.get(Constants.OVER_ALL_PROVIDER_LIMIT)).longValue();
            Map<String, Object> params = new HashMap<>();
            params.put(Constants.PARTNER_NAME, partnerName);
            params.put(Constants.THRESHOLD, threshold);
            params.put(Constants.CONSUMED, consumed);
            params.put(Constants.OVER_ALL_PROVIDER_LIMIT, overAllLimit);
            params.put(Constants.REMAINING, Math.max(0L, overAllLimit - consumed));
            params.put(Constants.CONSUMED_PCT, overAllLimit > 0 ? consumed * 100 / overAllLimit : 0L);
            params.put(Constants.DATE_TIME, ZonedDateTime.now(ZoneId.of(IST_ZONE)).format(DATE_TIME_FORMAT));
            boolean exhausted = threshold >= EXHAUSTED_THRESHOLD;
            String templateName = exhausted
                    ? Constants.EMAIL_TEMPLATE_LICENCE_EXHAUSTED : Constants.EMAIL_TEMPLATE_LICENCE_THRESHOLD_CROSSED;
            String subject = exhausted
                    ? String.format(Constants.LICENCE_ALERT_EXHAUSTED_SUBJECT, partnerName)
                    : String.format(Constants.LICENCE_ALERT_THRESHOLD_SUBJECT, partnerName, threshold);
            sendEmail((List<String>) event.get(Constants.RECIPIENTS), templateName, subject,
                    renderTemplate(templateName, params), params);
            log.info("Licence alert {}% sent for partnerId: {}", threshold, partnerId);
        } catch (Exception e) {
            log.error("Failed to send licence alert {}% for partnerId: {}", threshold, partnerId, e);
            cacheService.deleteCache(alertKey(partnerId, threshold), cbServerProperties.getDefaultIndex());
        }
    }

    private void publishAlert(String partnerId, JsonNode providerData, long consumed, long overAllLimit,
                              List<Integer> newlyReached, List<String> recipients) {
        Map<String, Object> event = new HashMap<>();
        event.put(Constants.PARTNER_ID, partnerId);
        event.put(Constants.PARTNER_NAME, providerData.path(Constants.CONTENT_PARTNER_NAME).asText(""));
        event.put(Constants.THRESHOLD, Collections.max(newlyReached));
        event.put(Constants.CONSUMED, consumed);
        event.put(Constants.OVER_ALL_PROVIDER_LIMIT, overAllLimit);
        event.put(Constants.RECIPIENTS, recipients);
        try {
            producer.push(cbServerProperties.getProviderLicenceAlertTopic(), event, partnerId);
        } catch (Exception e) {
            log.error("Failed to publish licence alert for partnerId: {}", partnerId, e);
            newlyReached.forEach(threshold ->
                    cacheService.deleteCache(alertKey(partnerId, threshold), cbServerProperties.getDefaultIndex()));
        }
    }

    /**
     * The provider's notificationdata and alertthresholds, or empty when nothing is saved.
     */
    private Map<String, Object> readSettings(String partnerId) {
        List<Map<String, Object>> records = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                Constants.KEYSPACE_SUNBIRD_COURSES, Constants.PROVIDER_ACCESS_SETTINGS_TABLE,
                Map.of(Constants.PARTNER_ID_REQ, partnerId),
                List.of(Constants.NOTIFICATION_DATA, Constants.ALERT_THRESHOLDS), 1);
        return CollectionUtils.isEmpty(records) ? Map.of() : records.get(0);
    }

    /**
     * The provider's saved thresholds that are on (alertthresholds: threshold to on/off), in
     * ascending order, or the configured defaults until it saves its own.
     */
    private List<Integer> thresholdsOf(Map<String, Object> settings) {
        if (settings.get(Constants.ALERT_THRESHOLDS) instanceof Map<?, ?> saved && !saved.isEmpty()) {
            return saved.entrySet().stream()
                    .filter(threshold -> Boolean.TRUE.equals(threshold.getValue()))
                    .map(threshold -> ((Number) threshold.getKey()).intValue())
                    .sorted()
                    .toList();
        }
        return getThresholds();
    }

    /**
     * The recipients' emails from the provider's notificationdata, or empty when alerts are off.
     */
    private List<String> recipientsOf(Map<String, Object> settings) throws JsonProcessingException {
        if (!(settings.get(Constants.NOTIFICATION_DATA) instanceof String json) || StringUtils.isBlank(json)) {
            return List.of();
        }
        JsonNode notificationData = objectMapper.readTree(json);
        List<String> emails = new ArrayList<>();
        if (notificationData.path(Constants.ALERTS_ENABLED).asBoolean(false)) {
            notificationData.path(Constants.RECIPIENTS).forEach(recipient -> {
                String email = recipient.path(Constants.EMAIL).asText("");
                if (StringUtils.isNotBlank(email)) {
                    emails.add(email);
                }
            });
        }
        return emails;
    }

    private String renderTemplate(String templateName, Map<String, Object> params) {
        List<Map<String, Object>> records = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                Constants.KEYSPACE_SUNBIRD, Constants.TABLE_EMAIL_TEMPLATE,
                Map.of(Constants.NAME, templateName), List.of(Constants.TEMPLATE), 1);
        if (CollectionUtils.isEmpty(records) || !(records.get(0).get(Constants.TEMPLATE) instanceof String template)) {
            throw new IllegalStateException("Email template not found: " + templateName);
        }
        VelocityEngine velocityEngine = new VelocityEngine();
        velocityEngine.init();
        VelocityContext context = new VelocityContext();
        params.forEach(context::put);
        StringWriter writer = new StringWriter();
        velocityEngine.evaluate(context, writer, templateName, template);
        return writer.toString();
    }

    /**
     * Same request shape as the other services' email notifications (notification service v2):
     * no "to" recipients, every recipient in bccIds.
     */
    private void sendEmail(List<String> recipients, String templateName, String subject, String body,
                           Map<String, Object> params) {
        Map<String, Object> config = new HashMap<>();
        config.put(Constants.SUBJECT, subject);
        config.put(Constants.SENDER, cbServerProperties.getProviderLicenceAlertSender());

        Map<String, Object> template = new HashMap<>();
        template.put(Constants.DATA, body);
        template.put(Constants.ID, templateName);
        template.put(Constants.PARAMS, params);
        template.put(Constants.TYPE, Constants.EMAIL);
        template.put(Constants.CONFIG, config);

        Map<String, Object> action = new HashMap<>();
        action.put(Constants.TYPE, Constants.EMAIL);
        action.put(Constants.CATEGORY, Constants.EMAIL);
        action.put(Constants.CREATED_BY, Map.of(Constants.ID, Constants.SYSTEM, Constants.TYPE, Constants.SYSTEM));
        action.put(Constants.TEMPLATE, template);

        Map<String, Object> notification = new HashMap<>();
        notification.put(Constants.TYPE, Constants.EMAIL);
        notification.put(Constants.PRIORITY, 1);
        notification.put(Constants.IDS, Collections.emptyList());
        notification.put(Constants.BCC_IDS, recipients);
        notification.put(Constants.ACTION, action);

        Map<String, Object> request = Map.of(Constants.REQUEST,
                Map.of(Constants.NOTIFICATIONS, Collections.singletonList(notification)));
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<String> response = restTemplate.postForEntity(
                cbServerProperties.getNotificationServiceBaseUrl() + cbServerProperties.getNotificationServiceSendApiUrl(),
                new HttpEntity<>(request, headers), String.class);
        if (!response.getStatusCode().is2xxSuccessful()) {
            throw new IllegalStateException("Notification service returned " + response.getStatusCode());
        }
    }

    private List<Integer> getThresholds() {
        return cbServerProperties.getProviderLicenceAlertThresholdsList().stream()
                .map(String::trim)
                .filter(StringUtils::isNotBlank)
                .map(Integer::parseInt)
                .sorted()
                .toList();
    }

    private String alertKey(String partnerId, int threshold) {
        return Constants.LICENCE_ALERT_FIRED_KEY_PREFIX + partnerId + "_" + threshold;
    }
}
