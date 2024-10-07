package com.igot.cb.util;


import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@Getter
@Setter
public class CbServerProperties {
    @Value("${redis.cache.enabled}")
    private boolean redisCacheEnable;

    @Value("${svgTemplate}")
    private String svgTemplate;

    @Value("${cios.read.api.base.url}")
    private String baseUrl;

    @Value("${cios.read.api.fixed.url}")
    private String ciosReadApiUrl;

    @Value("${kong.api.auth.token}")
    private String token;

    @Value("${spring.kafka.certificate.topic.name}")
    private String certificateTopic;

    @Value("${certificate.char.length}")
    private int certificateCharLength;

    @Value("${content.partner.read.api.url}")
    private String contentPartnerReadApiUrl;

    @Value("${spring.kafka.cornell.topic.name}")
    private String userProgressUpdateTopic;
}
