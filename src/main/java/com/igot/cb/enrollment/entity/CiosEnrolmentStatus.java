package com.igot.cb.enrollment.entity;

import java.util.HashMap;
import java.util.Map;

public enum CiosEnrolmentStatus {

    IN_PROGRESS("In-Progress", 0),
    COMPLETED("Completed", 2),
    PENDING("Pending", 3),
    ALL("All", -1);

    private final String label;
    private final int code;

    CiosEnrolmentStatus(String label, int code) {
        this.label = label;
        this.code = code;
    }

    public String getLabel() {
        return label;
    }

    public int getCode() {
        return code;
    }

    public static Map<String, Integer> toMap() {
        Map<String, Integer> map = new HashMap<>();
        for (CiosEnrolmentStatus status : values()) {
            map.put(status.label, status.code);
        }
        return map;
    }
}
