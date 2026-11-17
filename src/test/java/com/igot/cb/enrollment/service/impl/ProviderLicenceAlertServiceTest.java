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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
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
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(eq(Constants.KEYSPACE_SUNBIRD_COURSES),
                eq(Constants.PROVIDER_ACCESS_SETTINGS_TABLE), eq(Map.of(Constants.PARTNER_ID_REQ, "p1")),
                eq(List.of(Constants.NOTIFICATION_DATA, Constants.ALERT_THRESHOLDS)), eq(1))).thenReturn(List.of(row));
    }

    private static final String ALERTS_ON =
            "{\"alertsEnabled\":true,\"recipients\":[{\"name\":\"A\",\"email\":\"a@x.com\"},{\"email\":\"b@x.com\"},{\"email\":\"\"}]}";

    private String key(int threshold) {
        return Constants.LICENCE_ALERT_FIRED_KEY_PREFIX + "p1_" + threshold;
    }

    @Test
    void checkThresholds_NoOverAllLimit_DoesNothing() {
        alertService.checkThresholds("p1", provider(0), 10);

        verifyNoInteractions(cacheService, cassandraOperation, producer);
    }

    @Test
    void checkThresholds_BelowAllThresholds_RearmsAllDefaultThresholds() {
        alertService.checkThresholds("p1", provider(100), 49);

        for (int threshold : List.of(50, 75, 90, 100)) {
            verify(cacheService).deleteCache(key(threshold), 0);
        }
        verify(cacheService, never()).setIfAbsent(anyString(), anyInt(), anyString());
        verifyNoInteractions(producer);
    }

    @Test
    void checkThresholds_SavedThresholds_UsedInsteadOfDefaults() {
        stubSettings(ALERTS_ON, Map.of(80, true, 60, true));
        when(cacheService.setIfAbsent(eq(key(60)), eq(0), anyString())).thenReturn(true);

        alertService.checkThresholds("p1", provider(100), 65);

        verify(cacheService).deleteCache(key(80), 0);
        verify(cacheService, never()).deleteCache(key(50), 0);
        verify(cacheService, never()).setIfAbsent(eq(key(50)), anyInt(), anyString());
        verify(producer).push(eq(TOPIC), argThat(event ->
                Integer.valueOf(60).equals(((Map<?, ?>) event).get(Constants.THRESHOLD))), eq("p1"));
    }

    @Test
    void checkThresholds_EmptySavedThresholds_UsesDefaults() {
        stubSettings(ALERTS_ON, Map.of());

        alertService.checkThresholds("p1", provider(100), 10);

        for (int threshold : List.of(50, 75, 90, 100)) {
            verify(cacheService).deleteCache(key(threshold), 0);
        }
        verifyNoInteractions(producer);
    }

    @Test
    void checkThresholds_ThresholdsSwitchedOff_NeverMarkedSentOrRearmed() {
        stubSettings(ALERTS_ON, Map.of(50, true, 75, false, 90, false));
        when(cacheService.setIfAbsent(eq(key(50)), eq(0), anyString())).thenReturn(true);

        alertService.checkThresholds("p1", provider(100), 80);

        verify(producer).push(eq(TOPIC), argThat(event ->
                Integer.valueOf(50).equals(((Map<?, ?>) event).get(Constants.THRESHOLD))), eq("p1"));
        for (int threshold : List.of(75, 90, 100)) {
            verify(cacheService, never()).setIfAbsent(eq(key(threshold)), anyInt(), anyString());
            verify(cacheService, never()).deleteCache(key(threshold), 0);
        }
    }

    @Test
    void checkThresholds_EveryThresholdOff_SendsNothing() {
        stubSettings(ALERTS_ON, Map.of(50, false, 100, false));

        alertService.checkThresholds("p1", provider(100), 100);

        verify(cacheService, never()).setIfAbsent(anyString(), anyInt(), anyString());
        verify(cacheService, never()).deleteCache(anyString(), anyInt());
        verifyNoInteractions(producer);
    }

    @Test
    void checkThresholds_ThresholdAddedBelowOneAlreadySent_MarksWithoutPublishing() {
        stubSettings(ALERTS_ON, Map.of(50, true, 60, true, 75, true));
        when(cacheService.setIfAbsent(eq(key(50)), eq(0), anyString())).thenReturn(false);
        when(cacheService.setIfAbsent(eq(key(60)), eq(0), anyString())).thenReturn(true);
        when(cacheService.setIfAbsent(eq(key(75)), eq(0), anyString())).thenReturn(false);

        alertService.checkThresholds("p1", provider(100), 80);

        verify(cacheService).setIfAbsent(eq(key(60)), eq(0), anyString());
        verifyNoInteractions(producer);
    }

    @Test
    void checkThresholds_CrossesNewThreshold_PublishesHighestNewlySet() {
        stubNotificationData(ALERTS_ON);
        when(cacheService.setIfAbsent(eq(key(50)), eq(0), anyString())).thenReturn(false);
        when(cacheService.setIfAbsent(eq(key(75)), eq(0), anyString())).thenReturn(true);

        alertService.checkThresholds("p1", provider(1000), 760);

        verify(cacheService).deleteCache(key(90), 0);
        verify(cacheService).deleteCache(key(100), 0);
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
    void checkThresholds_AllReachedAlreadySent_DoesNotPublish() {
        stubNotificationData(ALERTS_ON);
        when(cacheService.setIfAbsent(anyString(), anyInt(), anyString())).thenReturn(false);

        alertService.checkThresholds("p1", provider(100), 100);

        verify(cacheService, never()).deleteCache(anyString(), anyInt());
        verifyNoInteractions(producer);
    }

    @Test
    void checkThresholds_AlertsOff_MarksNothing() {
        stubNotificationData("{\"alertsEnabled\":false,\"recipients\":[{\"email\":\"a@x.com\"}]}");

        alertService.checkThresholds("p1", provider(100), 80);

        verify(cacheService, never()).setIfAbsent(anyString(), anyInt(), anyString());
        verifyNoInteractions(producer);
    }

    @Test
    void checkThresholds_NoSettingsOrBlankData_MarksNothing() {
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(any(), any(), any(), any(), any()))
                .thenReturn(List.of())
                .thenReturn(List.of(new HashMap<>(Map.of(Constants.NOTIFICATION_DATA, " "))));

        alertService.checkThresholds("p1", provider(100), 80);
        alertService.checkThresholds("p1", provider(100), 80);

        verify(cacheService, never()).setIfAbsent(anyString(), anyInt(), anyString());
        verifyNoInteractions(producer);
    }

    @Test
    void checkThresholds_PublishFails_ClearsTheKeysItSet() {
        stubNotificationData(ALERTS_ON);
        when(cacheService.setIfAbsent(anyString(), anyInt(), anyString())).thenReturn(true);
        doThrow(new RuntimeException("kafka down")).when(producer).push(anyString(), any(), anyString());

        alertService.checkThresholds("p1", provider(100), 95);

        verify(cacheService).deleteCache(key(50), 0);
        verify(cacheService).deleteCache(key(75), 0);
        verify(cacheService).deleteCache(key(90), 0);
        // 100 was below the consumption: re-armed once, never set
        verify(cacheService).deleteCache(key(100), 0);
        verify(cacheService, never()).setIfAbsent(eq(key(100)), anyInt(), anyString());
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
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_EMAIL_TEMPLATE), eq(Map.of(Constants.NAME, name)), eq(List.of(Constants.TEMPLATE)), eq(1)))
                .thenReturn(List.of(Map.of(Constants.TEMPLATE, template)));
    }

    @Test
    @SuppressWarnings("unchecked")
    void sendAlert_ThresholdCrossed_SendsTemplateEmailInBcc() {
        stubTemplate(Constants.EMAIL_TEMPLATE_LICENCE_THRESHOLD_CROSSED,
                "Dear Karmayogi, $partnerName used $consumedPct% crossing $threshold%: $consumed/$overAllLimit, left $remaining ($dateTime)");
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
        assertTrue(body.startsWith("Dear Karmayogi, SIMPLILEARN used 76% crossing 75%: 30400/40000, left 9600 ("), body);
        assertTrue(body.endsWith(" IST)"), body);
        verify(cacheService, never()).deleteCache(anyString(), anyInt());
    }

    @Test
    void sendAlert_Exhausted_UsesExhaustedTemplateAndSubject() {
        stubTemplate(Constants.EMAIL_TEMPLATE_LICENCE_EXHAUSTED, "All $overAllLimit used");
        when(restTemplate.postForEntity(eq(NOTIFICATION_URL), any(HttpEntity.class), eq(String.class)))
                .thenReturn(new ResponseEntity<>("{}", HttpStatus.OK));

        alertService.sendAlert(alertEvent(100, 40000));

        verify(restTemplate).postForEntity(eq(NOTIFICATION_URL), argThat((HttpEntity<?> entity) ->
                entity.getBody().toString().contains("[iGOT Karmayogi] Action needed: licences exhausted for SIMPLILEARN")
                        && entity.getBody().toString().contains("All 40000 used")), eq(String.class));
    }

    @Test
    void sendAlert_TemplateMissing_RearmsThreshold() {
        when(cassandraOperation.getRecordsByPropertiesWithoutFiltering(any(), any(), any(), any(), any())).thenReturn(List.of());

        alertService.sendAlert(alertEvent(90, 36000));

        verify(cacheService).deleteCache(key(90), 0);
        verifyNoInteractions(restTemplate);
    }

    @Test
    void sendAlert_NotificationServiceFails_RearmsThreshold() {
        stubTemplate(Constants.EMAIL_TEMPLATE_LICENCE_THRESHOLD_CROSSED, "body");
        when(restTemplate.postForEntity(eq(NOTIFICATION_URL), any(HttpEntity.class), eq(String.class)))
                .thenReturn(new ResponseEntity<>("{}", HttpStatus.BAD_GATEWAY));

        alertService.sendAlert(alertEvent(50, 20000));

        verify(cacheService).deleteCache(key(50), 0);
    }
}
