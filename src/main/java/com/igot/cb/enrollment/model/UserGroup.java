package com.igot.cb.enrollment.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Setter
@Getter
@JsonIgnoreProperties(ignoreUnknown = true)
public class UserGroup {

    @JsonProperty("userGroupId")
    private String userGroupId;

    @JsonProperty("userGroupName")
    private String userGroupName;

    @JsonProperty("userGroupCriteriaList")
    private List<UserGroupCriteria> userGroupCriteriaList;
}
