package com.igot.cb.enrollment.entity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.*;

class CiosContentEntityTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void testAllArgsConstructorAndGetters() throws Exception {
        JsonNode jsonNode = mapper.readTree("{\"key\": \"value\"}");
        Timestamp created = new Timestamp(System.currentTimeMillis());
        Timestamp updated = new Timestamp(System.currentTimeMillis());

        CiosContentEntity entity = new CiosContentEntity(
                "content123",
                "ext-001",
                jsonNode,
                true,
                created,
                updated,
                "partnerXYZ"
        );

        assertEquals("content123", entity.getContentId());
        assertEquals("ext-001", entity.getExternalId());
        assertEquals(jsonNode, entity.getCiosData());
        assertTrue(entity.isActive());
        assertEquals(created, entity.getCreatedOn());
        assertEquals(updated, entity.getLastUpdatedOn());
        assertEquals("partnerXYZ", entity.getPartnerId());
    }

    @Test
    void testNoArgsConstructorAndSetters() throws Exception {
        CiosContentEntity entity = new CiosContentEntity();
        JsonNode jsonNode = mapper.readTree("{\"foo\": \"bar\"}");
        Timestamp created = new Timestamp(System.currentTimeMillis());
        Timestamp updated = new Timestamp(System.currentTimeMillis());

        entity.setContentId("c456");
        entity.setExternalId("ext-456");
        entity.setCiosData(jsonNode);
        entity.setActive(false);
        entity.setCreatedOn(created);
        entity.setLastUpdatedOn(updated);
        entity.setPartnerId("partner123");

        assertEquals("c456", entity.getContentId());
        assertEquals("ext-456", entity.getExternalId());
        assertEquals(jsonNode, entity.getCiosData());
        assertFalse(entity.isActive());
        assertEquals(created, entity.getCreatedOn());
        assertEquals(updated, entity.getLastUpdatedOn());
        assertEquals("partner123", entity.getPartnerId());
    }

    @Test
    void testSerialization() throws Exception {
        CiosContentEntity entity = new CiosContentEntity();
        entity.setContentId("serialize-test");

        // Serialize
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(entity);
        oos.flush();
        oos.close();

        // Deserialize
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        CiosContentEntity deserialized = (CiosContentEntity) ois.readObject();

        assertEquals("serialize-test", deserialized.getContentId());
    }
}

