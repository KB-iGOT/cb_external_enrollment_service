package com.igot.cb.enrollment.entity;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CiosEnrolmentStatusTest {

    @Test
    void testGetLabel() {
        // Act & Assert
        assertEquals("In-Progress", CiosEnrolmentStatus.IN_PROGRESS.getLabel());
        assertEquals("Completed", CiosEnrolmentStatus.COMPLETED.getLabel());
        assertEquals("All", CiosEnrolmentStatus.ALL.getLabel());
    }

    @Test
    void testGetCode() {
        // Act & Assert
        assertEquals(0, CiosEnrolmentStatus.IN_PROGRESS.getCode());
        assertEquals(2, CiosEnrolmentStatus.COMPLETED.getCode());
        assertEquals(-1, CiosEnrolmentStatus.ALL.getCode());
    }

    @Test
    void testToMap() {
        // Act
        Map<String, Integer> map = CiosEnrolmentStatus.toMap();
        
        // Assert
        assertEquals(3, map.size());
        assertEquals(0, map.get("In-Progress"));
        assertEquals(2, map.get("Completed"));
        assertEquals(-1, map.get("All"));
    }
}