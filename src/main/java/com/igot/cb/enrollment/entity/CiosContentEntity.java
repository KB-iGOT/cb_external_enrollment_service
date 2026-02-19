package com.igot.cb.enrollment.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.vladmihalcea.hibernate.type.json.JsonType;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.Type;
import java.io.Serial;
import java.io.Serializable;
import java.sql.Timestamp;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Entity
@JsonIgnoreProperties(ignoreUnknown = true)
public class CiosContentEntity implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    @Id
    private String contentId;
    private String externalId;
    @Column(columnDefinition = "jsonb")
    @Type(JsonType.class)
    private JsonNode ciosData;
    private boolean isActive;
    private Timestamp createdOn;
    private Timestamp lastUpdatedOn;
    private String partnerId;
}
