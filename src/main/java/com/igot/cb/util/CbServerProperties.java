package com.igot.cb.util;


import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;

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


    @Value("${content.partner.readby.partnercode.api.url}")
    private String contentPartnerReadbyPartnerCodeApiUrl;

    @Value("${spring.kafka.cornell.topic.name}")
    private String userProgressUpdateTopic;

    @Value("${user.progress.send.from.partner.topic.name}")
    private String userProgressSendFromPartner;

    @Value("${maximum.allowed.limit}")
    private int maximumAllowedLimit;

    @Value("${cios.content.read.api.fixed.url}")
    private String ciosContentReadApiUrl;

    @Value("${lms.enrolment.summary.base.url}")
    private String lmsEnrolmentSummaryBaseUrl;

    @Value("${lms.enrolment.summary.fixed.url}")
    private String lmsEnrolmentSummaryFixedUrl;

    @Value("${enrollment.karma.points.exempt.groups}")
    private List<String> karmaExemptGroups;

    @Value("${enrollment.error.partner.overall.limit}")
    private String partnerOverallLimitMsg;

    @Value("${enrollment.error.partner.userwise.limit}")
    private String partnerUserwiseLimitMsg;

    @Value("${enrollment.error.partner.concurrent.limit}")
    private String partnerConcurrentLimitMsg;

    @Value("${enrollment.error.karma.insufficient}")
    private String karmaInsufficientMsg;

    @Value("${access.settings.error.message}")
    private String accessSettingsErrorMessage;

}
