package com.igot.cb.util;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(classes = CbServerProperties.class)
@TestPropertySource(properties = {
        "redis.cache.enabled=true",
        "svgTemplate=test-template",
        "cios.read.api.base.url=http://base.url",
        "cios.read.api.fixed.url=http://fixed.url",
        "kong.api.auth.token=secret-token",
        "spring.kafka.certificate.topic.name=certificate-topic",
        "certificate.char.length=15",
        "content.partner.read.api.url=http://partner.url",
        "content.partner.readby.partnercode.api.url=http://partner.code.url",
        "spring.kafka.cornell.topic.name=user-progress-topic",
        "user.progress.send.from.partner.topic.name=user-partner-progress",
        "maximum.allowed.limit=500",
        "spring.redis.index=1",
        "spring.redis.default.index=0",
        "cios.content.read.api.fixed.url=http://content.read.url",
        "lms.enrolment.summary.base.url=http://lms.summary.base",
        "lms.enrolment.summary.fixed.url=http://lms.summary.fixed",
        "enrollment.karma.points.exempt.groups=group1,group2",
        "enrollment.error.partner.overall.limit=Overall limit reached",
        "enrollment.error.partner.userwise.limit=User limit reached",
        "enrollment.error.partner.concurrent.limit=Concurrent limit reached",
        "enrollment.error.karma.insufficient=Insufficient karma",
        "access.settings.error.message=Access denied",
        "coursera.partner.code=COURSERA",
        "service.registry.api.base.url=http://registry.base",
        "service.registry.api.fixed.url=http://registry.fixed",
        "coursera.service.code=SRV001",
        "coursera.org.id=ORG001"
})
class CbServerPropertiesTest {

    @Autowired
    private CbServerProperties properties;

    @Test
    void testAllPropertiesAreInjectedAndSettersWork() {
        // Verify Injected Values
        assertTrue(properties.isRedisCacheEnable());


        assertAll("Injected values",
                () -> assertTrue(properties.isRedisCacheEnable()),
                () -> assertEquals("test-template", properties.getSvgTemplate()),
                () -> assertEquals("http://base.url", properties.getBaseUrl()),
                () -> assertEquals("http://fixed.url", properties.getCiosReadApiUrl()),
                () -> assertEquals("secret-token", properties.getToken()),
                () -> assertEquals("certificate-topic", properties.getCertificateTopic()),
                () -> assertEquals(15, properties.getCertificateCharLength()),
                () -> assertEquals("http://partner.url", properties.getContentPartnerReadApiUrl()),
                () -> assertEquals("http://partner.code.url", properties.getContentPartnerReadbyPartnerCodeApiUrl()),
                () -> assertEquals("user-progress-topic", properties.getUserProgressUpdateTopic()),
                () -> assertEquals("user-partner-progress", properties.getUserProgressSendFromPartner()),
                () -> assertEquals(500, properties.getMaximumAllowedLimit()),
                () -> assertEquals(1, properties.getRedisIndex()),
                () -> assertEquals(0, properties.getDefaultIndex())
        );

        assertAll("Injected values1",
                () -> assertTrue(properties.isRedisCacheEnable()),
                () -> assertEquals("test-template", properties.getSvgTemplate()),
                () -> assertEquals("http://base.url", properties.getBaseUrl()),
                () -> assertEquals("http://fixed.url", properties.getCiosReadApiUrl()),
                () -> assertEquals("secret-token", properties.getToken()),
                () -> assertEquals("certificate-topic", properties.getCertificateTopic()),
                () -> assertEquals(15, properties.getCertificateCharLength()),
                () -> assertEquals("http://partner.url", properties.getContentPartnerReadApiUrl()),
                () -> assertEquals("http://partner.code.url", properties.getContentPartnerReadbyPartnerCodeApiUrl()),
                () -> assertEquals("user-progress-topic", properties.getUserProgressUpdateTopic()),
                () -> assertEquals("user-partner-progress", properties.getUserProgressSendFromPartner()),
                () -> assertEquals(500, properties.getMaximumAllowedLimit()),
                () -> assertEquals(1, properties.getRedisIndex()),
                () -> assertEquals(0, properties.getDefaultIndex())
        );

        assertAll("Injected values1",
                () -> assertEquals("http://content.read.url", properties.getCiosContentReadApiUrl()),
                () -> assertEquals("http://lms.summary.base", properties.getLmsEnrolmentSummaryBaseUrl()),
                () -> assertEquals("http://lms.summary.fixed", properties.getLmsEnrolmentSummaryFixedUrl()),
                () -> assertEquals(2, properties.getKarmaExemptGroups().size()),
                () -> assertTrue(properties.getKarmaExemptGroups().contains("group1")),
                () -> assertEquals("Overall limit reached", properties.getPartnerOverallLimitMsg()),
                () -> assertEquals("User limit reached", properties.getPartnerUserwiseLimitMsg()),
                () -> assertEquals("Concurrent limit reached", properties.getPartnerConcurrentLimitMsg()),
                () -> assertEquals("Insufficient karma", properties.getKarmaInsufficientMsg()),
                () -> assertEquals("Access denied", properties.getAccessSettingsErrorMessage()),
                () -> assertEquals("COURSERA", properties.getCourseraPartnerCode()),
                () -> assertEquals("http://registry.base", properties.getServiceRegistryApiBaseUrl()),
                () -> assertEquals("http://registry.fixed", properties.getServiceRegistryApiFixedUrl()),
                () -> assertEquals("SRV001", properties.getCourseraServiceCode()),
                () -> assertEquals("ORG001", properties.getCourseraOrgId())
        );


        // Verify Setters
        properties.setRedisCacheEnable(false);
        assertEquals(false, properties.isRedisCacheEnable());

        properties.setSvgTemplate("new-template");
        assertEquals("new-template", properties.getSvgTemplate());

        properties.setBaseUrl("new-base");
        assertEquals("new-base", properties.getBaseUrl());

        properties.setCiosReadApiUrl("new-cios-read");
        assertEquals("new-cios-read", properties.getCiosReadApiUrl());

        properties.setToken("new-token");
        assertEquals("new-token", properties.getToken());

        properties.setCertificateTopic("new-cert");
        assertEquals("new-cert", properties.getCertificateTopic());

        properties.setCertificateCharLength(20);
        assertEquals(20, properties.getCertificateCharLength());

        properties.setContentPartnerReadApiUrl("new-partner-read");
        assertEquals("new-partner-read", properties.getContentPartnerReadApiUrl());

        properties.setContentPartnerReadbyPartnerCodeApiUrl("new-partner-code");
        assertEquals("new-partner-code", properties.getContentPartnerReadbyPartnerCodeApiUrl());

        properties.setUserProgressUpdateTopic("new-progress");
        assertEquals("new-progress", properties.getUserProgressUpdateTopic());

        properties.setUserProgressSendFromPartner("new-sender");
        assertEquals("new-sender", properties.getUserProgressSendFromPartner());

        properties.setMaximumAllowedLimit(100);
        assertEquals(100, properties.getMaximumAllowedLimit());

        properties.setRedisIndex(2);
        assertEquals(2, properties.getRedisIndex());

        properties.setDefaultIndex(1);
        assertEquals(1, properties.getDefaultIndex());

        properties.setCiosContentReadApiUrl("new-content-read");
        assertEquals("new-content-read", properties.getCiosContentReadApiUrl());

        properties.setLmsEnrolmentSummaryBaseUrl("new-lms-base");
        assertEquals("new-lms-base", properties.getLmsEnrolmentSummaryBaseUrl());

        properties.setLmsEnrolmentSummaryFixedUrl("new-lms-fixed");
        assertEquals("new-lms-fixed", properties.getLmsEnrolmentSummaryFixedUrl());

        properties.setKarmaExemptGroups(null);
        assertEquals(null, properties.getKarmaExemptGroups());

        properties.setPartnerOverallLimitMsg("msg1");
        assertEquals("msg1", properties.getPartnerOverallLimitMsg());

        properties.setPartnerUserwiseLimitMsg("msg2");
        assertEquals("msg2", properties.getPartnerUserwiseLimitMsg());

        properties.setPartnerConcurrentLimitMsg("msg3");
        assertEquals("msg3", properties.getPartnerConcurrentLimitMsg());

        properties.setKarmaInsufficientMsg("msg4");
        assertEquals("msg4", properties.getKarmaInsufficientMsg());

        properties.setAccessSettingsErrorMessage("msg5");
        assertEquals("msg5", properties.getAccessSettingsErrorMessage());

        properties.setCourseraPartnerCode("C1");
        assertEquals("C1", properties.getCourseraPartnerCode());

        properties.setServiceRegistryApiBaseUrl("reg-base");
        assertEquals("reg-base", properties.getServiceRegistryApiBaseUrl());

        properties.setServiceRegistryApiFixedUrl("reg-fixed");
        assertEquals("reg-fixed", properties.getServiceRegistryApiFixedUrl());

        properties.setCourseraServiceCode("S1");
        assertEquals("S1", properties.getCourseraServiceCode());

        properties.setCourseraOrgId("O1");
        assertEquals("O1", properties.getCourseraOrgId());
    }
}
