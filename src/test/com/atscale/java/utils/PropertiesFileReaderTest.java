package com.atscale.java.utils;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.*;

import java.util.Collections;
import java.util.Map;

public class PropertiesFileReaderTest {

    @Test
    public void testGetCustomProperty() {
        Map<String, String> testProperties = Map.of("test.property", "testValue");
        PropertiesFileReader.setCustomProperties(testProperties);
        String value = PropertiesFileReader.getCustomProperty("test.property");
        Assertions.assertEquals("testValue", value);
    }

    @Test
    public void testGetCustomProperties() {
        Map<String, String> testProperties = Map.of("test.property", "testValue", "another.property", "anotherValue");
        PropertiesFileReader.setCustomProperties(testProperties);
        String value = PropertiesFileReader.getCustomProperty("test.property");
        Assertions.assertEquals("testValue", value);
        String anotherValue = PropertiesFileReader.getCustomProperty("another.property");
        Assertions.assertEquals("anotherValue", anotherValue);
    }

    @Test
    public void testCanSetEmptyMapWithoutError() {
        Map<String, String> testProperties = Collections.emptyMap();
        PropertiesFileReader.setCustomProperties(testProperties);
    }

    @Test
    public void testGettingNonExistentCustomPropertyThrowsError() {
        RuntimeException ex = Assertions.assertThrows(RuntimeException.class, () ->
                PropertiesFileReader.getCustomProperty(RandomStringUtils.secure().nextAlphabetic(35))
        );
    }
}
