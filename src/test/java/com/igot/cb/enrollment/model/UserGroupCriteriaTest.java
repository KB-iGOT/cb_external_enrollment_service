package com.igot.cb.enrollment.model;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class UserGroupCriteriaTest {

    @Test
    void testConstructorsAndGetters() {
        UserGroupCriteria criteria = new UserGroupCriteria();
        assertNull(criteria.getCriteriaKey());
        assertNull(criteria.getCriteriaValue());

        Set<String> values = new HashSet<>();
        values.add("value1");
        UserGroupCriteria criteria2 = new UserGroupCriteria("key1", values);
        assertEquals("key1", criteria2.getCriteriaKey());
        assertEquals(values, criteria2.getCriteriaValue());
    }

    @Test
    void testSetCriteriaKey_convertsToLowerCase() {
        UserGroupCriteria criteria = new UserGroupCriteria();
        criteria.setCriteriaKey("KeyName");
        assertEquals("keyname", criteria.getCriteriaKey());
    }

    @Test
    void testSetCriteriaValue_handling() {
        UserGroupCriteria criteria = new UserGroupCriteria();

        // Test null input
        criteria.setCriteriaValue(null);
        assertNotNull(criteria.getCriteriaValue());
        assertTrue(criteria.getCriteriaValue().isEmpty());

        // Test normal list with mixed case and nulls
        List<String> input = Arrays.asList("Value1", null, "VALUE2");
        criteria.setCriteriaValue(input);

        Set<String> result = criteria.getCriteriaValue();
        assertEquals(2, result.size());
        assertTrue(result.contains("value1"));
        assertTrue(result.contains("value2"));
    }

    @Test
    void testEvaluate_matchFound() {
        UserGroupCriteria criteria = new UserGroupCriteria();
        criteria.setCriteriaKey("department");
        criteria.setCriteriaValue(Collections.singletonList("engineering"));

        Map<String, String> userAttributes = new HashMap<>();
        userAttributes.put("department", "Engineering"); // Mixed case in user attribute

        assertTrue(criteria.evaluate(userAttributes));
    }

    @Test
    void testEvaluate_matchNotFound() {
        UserGroupCriteria criteria = new UserGroupCriteria();
        criteria.setCriteriaKey("department");
        criteria.setCriteriaValue(Collections.singletonList("engineering"));

        Map<String, String> userAttributes = new HashMap<>();
        userAttributes.put("department", "Sales");

        assertFalse(criteria.evaluate(userAttributes));
    }

    @Test
    void testEvaluate_missingAttribute() {
        UserGroupCriteria criteria = new UserGroupCriteria();
        criteria.setCriteriaKey("department");
        criteria.setCriteriaValue(Collections.singletonList("engineering"));

        Map<String, String> userAttributes = new HashMap<>();
        // Attribute not present

        assertFalse(criteria.evaluate(userAttributes));
    }

    @Test
    void testEvaluate_blankAttribute() {
        UserGroupCriteria criteria = new UserGroupCriteria();
        criteria.setCriteriaKey("department");
        criteria.setCriteriaValue(Collections.singletonList("engineering"));

        Map<String, String> userAttributes = new HashMap<>();
        userAttributes.put("department", "");

        assertFalse(criteria.evaluate(userAttributes));
    }
}
