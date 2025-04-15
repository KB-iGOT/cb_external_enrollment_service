
package com.igot.cb.transactional.cassandrautils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.cassandra.config.SchemaAction;

import static org.junit.jupiter.api.Assertions.*;

class CassandraConfigTest {

    // Concrete subclass with no overrides, so inherited functionality is tested.
    private static class TestCassandraConfig extends CassandraConfig { }

    private TestCassandraConfig config;

    @BeforeEach
    void setUp() {
        config = new TestCassandraConfig();
    }

    @Test
    void testSetAndGetContactPoints() {
        config.setContactPoints("192.168.1.100");
        assertEquals("192.168.1.100", config.getContactPoints());

        config.setContactPoints("localhost");
        assertEquals("localhost", config.getContactPoints());
    }

    @Test
    void testSetAndGetPort() {
        config.setPort(9042);
        assertEquals(9042, config.getPort());

        config.setPort(5000);
        assertEquals(5000, config.getPort());
    }

    @Test
    void testSetAndGetKeyspaceName() {
        config.setKeyspaceName("testspace");
        assertEquals("testspace", config.getKeyspaceName());

        config.setKeyspaceName("prodspace");
        assertEquals("prodspace", config.getKeyspaceName());
    }

    @Test
    void testDefaultValues() {
        // Should be null/0 if never set
        assertNull(config.getContactPoints());
        assertEquals(0, config.getPort());
        assertNull(config.getKeyspaceName());
    }

    @Test
    void testDefaultSchemaAction() {
        // The default from AbstractCassandraConfiguration is NONE
        assertEquals(SchemaAction.NONE, config.getSchemaAction());
    }
}
