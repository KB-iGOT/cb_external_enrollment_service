package com.igot.cb.enrollment.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.igot.cb.producer.Producer;
import com.igot.cb.transactional.cassandrautils.CassandraOperation;
import com.igot.cb.util.CbServerProperties;
import com.igot.cb.util.Constants;
import com.igot.cb.util.cache.CacheService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ProviderLicenceAlertServiceTest {

    private static final String TOPIC = "provider.licence.alert";
    private static final long TTL = 300L;
    private static final String NOTIFICATION_URL = "http://notification-service:9000/v2/notification/send";

    @InjectMocks
    private ProviderLicenceAlertService alertService;

    @Mock
    private CassandraOperation cassandraOperation;
    @Mock
    private CacheService cacheService;
    @Mock
    private CbServerProperties cbServerProperties;
    @Mock
    private Producer producer;
    @Mock
    private RestTemplate restTemplate;
    @Spy
    private ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        lenient().when(cbServerProperties.getProviderLicenceAlertThresholdsList()).thenReturn(List.of("100", " 50", "90", "75", ""));
        lenient().when(cbServerProperties.getDefaultIndex()).thenReturn(0);
        lenient().when(cbServerProperties.getProviderLicenceAlertLockTtlSeconds()).thenReturn(TTL);
        lenient().when(cbServerProperties.getProviderLicenceAlertTopic()).thenReturn(TOPIC);
        lenient().when(cbServerProperties.getProviderLicenceAlertSender()).thenReturn("support@igot.in");
        lenient().when(cbServerProperties.getNotificationServiceBaseUrl()).thenReturn("http://notification-service:9000");
        lenient().when(cbServerProperties.getNotificationServiceSendApiUrl()).thenReturn("/v2/notification/send");
    }

    private ObjectNode provider(long overAllLimit) {
        ObjectNode provider = objectMapper.createObjectNode();
        provider.put(Constants.OVER_ALL_PROVIDER_LIMIT, overAllLimit);
        provider.put(Constants.CONTENT_PARTNER_NAME, "SIMPLILEARN");
        return provider;
    }

    private void stubNotificationData(String json) {
        stubSettings(json, null);
    }

    private void stubSettings(String json, Map<Integer, Boolean> alertThresholds) {
        Map<String, Object> row = new HashMap<>();
        row.put(Constants.NOTIFICATION_DATA, json);
        row.put(Constants.ALERT_THRESHOLDS, alertThresholds);
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(Constants.KEYSPACE_SUNBIRD_COURSES,
                Constants.PROVIDER_ACCESS_SETTINGS_TABLE, Map.of(Constants.PARTNER_ID_REQ, "p1"),
                List.of(Constants.NOTIFICATION_DATA, Constants.ALERT_THRESHOLDS), 1)).thenReturn(List.of(row));
    }

    private static final String ALERTS_ON =
            "{\"alertsEnabled\":true,\"recipients\":[{\"name\":\"A\",\"email\":\"a@x.com\"},{\"email\":\"b@x.com\"},{\"email\":\"\"}]}";

    private String key(int threshold) {
        return Constants.LICENCE_ALERT_LOCK_KEY_PREFIX + "p1_" + threshold;
    }

    private void verifySaved(Map<Integer, Boolean> sentFlags) {
        verify(cassandraOperation).putMapEntries(Constants.KEYSPACE_SUNBIRD_COURSES,
                Constants.PROVIDER_ACCESS_SETTINGS_TABLE, Constants.ALERT_THRESHOLDS, sentFlags,
                Map.of(Constants.PARTNER_ID_REQ, "p1"));
    }

    private void verifyNothingSaved() {
        verify(cassandraOperation, never()).putMapEntries(anyString(), anyString(), anyString(), anyMap(), anyMap());
    }

    @Test
    void checkThresholds_NoOverAllLimit_DoesNothing() {
        alertService.checkThresholds("p1", provider(0), 10);

        verifyNoInteractions(cacheService, cassandraOperation, producer);
    }

    @Test
    void checkThresholds_BelowAllThresholds_SavesNothing() {
        alertService.checkThresholds("p1", provider(100), 49);

        verifyNothingSaved();
        verifyNoInteractions(cacheService, producer);
    }

    @Test
    void checkThresholds_EmptySavedThresholds_UsesDefaults() {
        stubSettings(ALERTS_ON, Map.of());
        when(cacheService.setIfAbsent(eq(key(50)), eq(0), anyString(), eq(TTL))).thenReturn(true);

        alertService.checkThresholds("p1", provider(100), 60);

        verifySaved(Map.of(50, true, 75, false, 90, false, 100, false));
    }

    @Test
    void checkThresholds_SentThresholdAboveConsumption_MarkedNotSentAndLockReleased() {
        stubSettings(ALERTS_ON, Map.of(50, true, 75, true, 90, false));

        alertService.checkThresholds("p1", provider(100), 60);

        verifySaved(Map.of(75, false));
        verify(cacheService).deleteCache(key(75), 0);
        verify(cacheService, never()).setIfAbsent(anyString(), anyInt(), anyString(), anyLong());
        verifyNoInteractions(producer);
    }

    @Test
    void checkThresholds_SavedThresholds_UsedInsteadOfDefaults() {
        stubSettings(ALERTS_ON, Map.of(80, false, 60, false));
        when(cacheService.setIfAbsent(eq(key(60)), eq(0), anyString(), eq(TTL))).thenReturn(true);

        alertService.checkThresholds("p1", provider(100), 65);

        verifySaved(Map.of(60, true));
        verify(cacheService, never()).setIfAbsent(eq(key(50)), anyInt(), anyString(), anyLong());
        verify(producer).push(eq(TOPIC), argThat(event ->
                Integer.valueOf(60).equals(((Map<?, ?>) event).get(Constants.THRESHOLD))), eq("p1"));
    }

    @Test
    void checkThresholds_NothingSaved_WritesDefaultsWholeAndPublishesHighest() {
        stubNotificationData(ALERTS_ON);
        when(cacheService.setIfAbsent(eq(key(50)), eq(0), anyString(), eq(TTL))).thenReturn(true);
        when(cacheService.setIfAbsent(eq(key(75)), eq(0), anyString(), eq(TTL))).thenReturn(true);

        alertService.checkThresholds("p1", provider(1000), 760);

        verifySaved(Map.of(50, true, 75, true, 90, false, 100, false));
        verify(producer).push(eq(TOPIC), argThat(event -> {
            Map<?, ?> map = (Map<?, ?>) event;
            return "p1".equals(map.get(Constants.PARTNER_ID))
                    && "SIMPLILEARN".equals(map.get(Constants.PARTNER_NAME))
                    && Integer.valueOf(75).equals(map.get(Constants.THRESHOLD))
                    && Long.valueOf(760).equals(map.get(Constants.CONSUMED))
                    && Long.valueOf(1000).equals(map.get(Constants.OVER_ALL_PROVIDER_LIMIT))
                    && List.of("a@x.com", "b@x.com").equals(map.get(Constants.RECIPIENTS));
        }), eq("p1"));
    }

    @Test
    void checkThresholds_ThresholdAddedBelowOneAlreadySent_MarksWithoutPublishing() {
        stubSettings(ALERTS_ON, Map.of(50, true, 60, false, 75, true));
        when(cacheService.setIfAbsent(eq(key(60)), eq(0), anyString(), eq(TTL))).thenReturn(true);

        alertService.checkThresholds("p1", provider(100), 80);

        verifySaved(Map.of(60, true));
        verify(cacheService, never()).setIfAbsent(eq(key(50)), anyInt(), anyString(), anyLong());
        verify(cacheService, never()).setIfAbsent(eq(key(75)), anyInt(), anyString(), anyLong());
        verifyNoInteractions(producer);
    }

    @Test
    void checkThresholds_AllReachedAlreadySent_DoesNothing() {
        stubSettings(ALERTS_ON, Map.of(50, true, 100, true));

        alertService.checkThresholds("p1", provider(100), 100);

        verifyNothingSaved();
        verifyNoInteractions(cacheService, producer);
    }

    @Test
    void checkThresholds_LockHeldByAnotherPod_SendsNothing() {
        stubSettings(ALERTS_ON, Map.of(50, false));
        when(cacheService.setIfAbsent(eq(key(50)), eq(0), anyString(), eq(TTL))).thenReturn(false);

        alertService.checkThresholds("p1", provider(100), 60);

        verifyNothingSaved();
        verifyNoInteractions(producer);
    }

    @Test
    void checkThresholds_AlertsOff_MarksNothing() {
        stubNotificationData("{\"alertsEnabled\":false,\"recipients\":[{\"email\":\"a@x.com\"}]}");

        alertService.checkThresholds("p1", provider(100), 80);

        verify(cacheService, never()).setIfAbsent(anyString(), anyInt(), anyString(), anyLong());
        verifyNothingSaved();
        verifyNoInteractions(producer);
    }

    @Test
    void checkThresholds_NoSettingsOrBlankData_MarksNothing() {
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(any(), any(), any(), any(), any()))
                .thenReturn(List.of())
                .thenReturn(List.of(new HashMap<>(Map.of(Constants.NOTIFICATION_DATA, " "))));

        alertService.checkThresholds("p1", provider(100), 80);
        alertService.checkThresholds("p1", provider(100), 80);

        verify(cacheService, never()).setIfAbsent(anyString(), anyInt(), anyString(), anyLong());
        verifyNothingSaved();
        verifyNoInteractions(producer);
    }

    @Test
    void checkThresholds_PublishFails_MarksThemNotSentAgain() {
        stubNotificationData(ALERTS_ON);
        when(cacheService.setIfAbsent(anyString(), anyInt(), anyString(), eq(TTL))).thenReturn(true);
        doThrow(new RuntimeException("kafka down")).when(producer).push(anyString(), any(), anyString());

        alertService.checkThresholds("p1", provider(100), 95);

        verifySaved(Map.of(50, true, 75, true, 90, true, 100, false));
        verifySaved(Map.of(50, false, 75, false, 90, false));
        for (int threshold : List.of(50, 75, 90)) {
            verify(cacheService).deleteCache(key(threshold), 0);
        }
        verify(cacheService, never()).setIfAbsent(eq(key(100)), anyInt(), anyString(), anyLong());
    }

    @Test
    void checkThresholds_ReadFails_NeverThrows() {
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(any(), any(), any(), any(), any()))
                .thenThrow(new RuntimeException("cassandra down"));

        alertService.checkThresholds("p1", provider(100), 100);

        verifyNoInteractions(producer);
    }

    private Map<String, Object> alertEvent(int threshold, long consumed) {
        Map<String, Object> event = new HashMap<>();
        event.put(Constants.PARTNER_ID, "p1");
        event.put(Constants.PARTNER_NAME, "SIMPLILEARN");
        event.put(Constants.THRESHOLD, threshold);
        event.put(Constants.CONSUMED, consumed);
        event.put(Constants.OVER_ALL_PROVIDER_LIMIT, 40000);
        event.put(Constants.RECIPIENTS, List.of("a@x.com", "b@x.com"));
        return event;
    }

    private void stubTemplate(String name, String template) {
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(Constants.KEYSPACE_SUNBIRD,
                Constants.TABLE_EMAIL_TEMPLATE, Map.of(Constants.NAME, name), List.of(Constants.TEMPLATE), 1))
                .thenReturn(List.of(Map.of(Constants.TEMPLATE, template)));
    }

    @Test
    @SuppressWarnings("unchecked")
    void sendAlert_ThresholdCrossed_SendsTemplateEmailInBcc() {
        stubTemplate(Constants.EMAIL_TEMPLATE_LICENCE_THRESHOLD_CROSSED,
                "Dear Karmayogi, $partnerName has used $threshold% of its overall licence limit.");
        when(restTemplate.postForEntity(eq(NOTIFICATION_URL), any(HttpEntity.class), eq(String.class)))
                .thenReturn(new ResponseEntity<>("{}", HttpStatus.OK));

        alertService.sendAlert(alertEvent(75, 30400));

        ArgumentCaptor<HttpEntity> captor = ArgumentCaptor.forClass(HttpEntity.class);
        verify(restTemplate).postForEntity(eq(NOTIFICATION_URL), captor.capture(), eq(String.class));
        Map<String, Object> request = (Map<String, Object>) ((Map<String, Object>) captor.getValue().getBody()).get(Constants.REQUEST);
        Map<String, Object> notification = ((List<Map<String, Object>>) request.get(Constants.NOTIFICATIONS)).get(0);
        assertEquals(List.of(), notification.get(Constants.IDS));
        assertEquals(List.of("a@x.com", "b@x.com"), notification.get(Constants.BCC_IDS));
        Map<String, Object> template = (Map<String, Object>) ((Map<String, Object>) notification.get(Constants.ACTION)).get(Constants.TEMPLATE);
        assertEquals(Constants.EMAIL_TEMPLATE_LICENCE_THRESHOLD_CROSSED, template.get(Constants.ID));
        assertEquals("[iGOT Karmayogi] SIMPLILEARN: 75% of licences used",
                ((Map<String, Object>) template.get(Constants.CONFIG)).get(Constants.SUBJECT));
        String body = (String) template.get(Constants.DATA);
        assertEquals("Dear Karmayogi, SIMPLILEARN has used 75% of its overall licence limit.", body);
        assertEquals(Map.of(Constants.PARTNER_NAME, "SIMPLILEARN", Constants.THRESHOLD, 75), template.get(Constants.PARAMS));
        verify(cacheService, never()).deleteCache(anyString(), anyInt());
        verifyNothingSaved();
    }

    @Test
    void sendAlert_Exhausted_UsesExhaustedTemplateAndSubject() {
        stubTemplate(Constants.EMAIL_TEMPLATE_LICENCE_EXHAUSTED, "All licences for $partnerName used");
        when(restTemplate.postForEntity(eq(NOTIFICATION_URL), any(HttpEntity.class), eq(String.class)))
                .thenReturn(new ResponseEntity<>("{}", HttpStatus.OK));

        alertService.sendAlert(alertEvent(100, 40000));

        verify(restTemplate).postForEntity(eq(NOTIFICATION_URL), argThat((HttpEntity<?> entity) ->
                entity.getBody().toString().contains("[iGOT Karmayogi] Action needed: licences exhausted for SIMPLILEARN")
                        && entity.getBody().toString().contains("All licences for SIMPLILEARN used")), eq(String.class));
    }

    @Test
    void sendAlert_TemplateMissing_RearmsThreshold() {
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(any(), any(), any(), any(), any())).thenReturn(List.of());

        alertService.sendAlert(alertEvent(90, 36000));

        verifySaved(Map.of(90, false));
        verify(cacheService).deleteCache(key(90), 0);
        verifyNoInteractions(restTemplate);
    }

    @Test
    void sendAlert_NotificationServiceFails_RearmsThreshold() {
        stubTemplate(Constants.EMAIL_TEMPLATE_LICENCE_THRESHOLD_CROSSED, "body");
        when(restTemplate.postForEntity(eq(NOTIFICATION_URL), any(HttpEntity.class), eq(String.class)))
                .thenReturn(new ResponseEntity<>("{}", HttpStatus.BAD_GATEWAY));

        alertService.sendAlert(alertEvent(50, 20000));

        verifySaved(Map.of(50, false));
        verify(cacheService).deleteCache(key(50), 0);
    }

    @Test
    void sendAlert_FailsAndMarkingNotSentFails_StillReleasesLock() {
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(any(), any(), any(), any(), any())).thenReturn(List.of());
        doThrow(new RuntimeException("cassandra down")).when(cassandraOperation)
                .putMapEntries(anyString(), anyString(), anyString(), anyMap(), anyMap());

        alertService.sendAlert(alertEvent(75, 30000));

        verify(cacheService).deleteCache(key(75), 0);
    }
}
