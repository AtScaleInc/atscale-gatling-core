package com.atscale.java.utils;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.*;

import java.util.Collections;
import java.util.Map;

public class PropertiesManagerTest {

    @Test
    public void testGetCustomProperty() {
        Map<String, String> testProperties = Map.of("test.property", "testValue");
        PropertiesManager.setCustomProperties(testProperties);
        String value = PropertiesManager.getCustomProperty("test.property");
        Assertions.assertEquals("testValue", value);
    }

    @Test
    public void testGetCustomProperties() {
        Map<String, String> testProperties = Map.of("test.property", "testValue", "another.property", "anotherValue");
        PropertiesManager.setCustomProperties(testProperties);
        String value = PropertiesManager.getCustomProperty("test.property");
        Assertions.assertEquals("testValue", value);
        String anotherValue = PropertiesManager.getCustomProperty("another.property");
        Assertions.assertEquals("anotherValue", anotherValue);
    }

    @Test
    public void testCanSetEmptyMapWithoutError() {
        Map<String, String> testProperties = Collections.emptyMap();
        PropertiesManager.setCustomProperties(testProperties);
    }

    @Test
    public void testGettingNonExistentCustomPropertyThrowsError() {
        Assertions.assertThrows(RuntimeException.class, () ->
                PropertiesManager.getCustomProperty(RandomStringUtils.secure().nextAlphabetic(35))
        );
    }
}
