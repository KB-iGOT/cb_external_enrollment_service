package com.igot.cb.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

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
        "spring.redis.default.index=0"
})
class CbServerPropertiesTest {

    @Autowired
    private CbServerProperties properties;

    @Test
    void testAllPropertiesAreInjected() {
        assertEquals(true, properties.isRedisCacheEnable());
        assertEquals("test-template", properties.getSvgTemplate());
        assertEquals("http://base.url", properties.getBaseUrl());
        assertEquals("http://fixed.url", properties.getCiosReadApiUrl());
        assertEquals("secret-token", properties.getToken());
        assertEquals("certificate-topic", properties.getCertificateTopic());
        assertEquals(15, properties.getCertificateCharLength());
        assertEquals("http://partner.url", properties.getContentPartnerReadApiUrl());
        assertEquals("http://partner.code.url", properties.getContentPartnerReadbyPartnerCodeApiUrl());
        assertEquals("user-progress-topic", properties.getUserProgressUpdateTopic());
        assertEquals("user-partner-progress", properties.getUserProgressSendFromPartner());
        assertEquals(500, properties.getMaximumAllowedLimit());
        assertEquals(1, properties.getRedisIndex());
        assertEquals(0, properties.getDefaultIndex());



        // Explicitly call setters to ensure setter methods are covered too
        properties.setRedisCacheEnable(false);
        assertEquals(false, properties.isRedisCacheEnable());

        properties.setSvgTemplate("new-template");
        assertEquals("new-template", properties.getSvgTemplate());

        properties.setRedisIndex(2);
        assertEquals(2, properties.getRedisIndex());

        properties.setDefaultIndex(1);
        assertEquals(1, properties.getDefaultIndex());

    }
}
