package com.company.kafka.config;

import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.*;

public class ConfigCreatorTest {

    private Properties prop;

    @Before
    public void setUp() throws Exception {
        prop = ConfigCreator.getConfig();
    }

    @Test
    public void getConfig() {
        assertTrue(prop != null);
        assertEquals("localhost:9092", prop.getProperty("bootstrap.servers"));
        assertEquals("false", prop.getProperty("enable.auto.commit"));
        assertEquals("earliest", prop.getProperty("auto.offset.reset"));
    }
}