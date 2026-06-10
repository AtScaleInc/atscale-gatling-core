package com.atscale.java.utils;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.*;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PropertiesManagerTest {

    @AfterEach
    void cleanup() {
        PropertiesManager.envReader = System::getenv;
        PropertiesManager.removeProperties(List.of(
                "test.property", "another.property", "env.only.property", "has.property.test"
        ));
    }

    // --- setCustomProperties / getCustomProperty ---

    @Test
    void testGetCustomProperty() {
        PropertiesManager.setCustomProperties(Map.of("test.property", "testValue"));
        Assertions.assertEquals("testValue", PropertiesManager.getCustomProperty("test.property"));
    }

    @Test
    void testGetCustomProperties() {
        PropertiesManager.setCustomProperties(Map.of("test.property", "testValue", "another.property", "anotherValue"));
        Assertions.assertEquals("testValue", PropertiesManager.getCustomProperty("test.property"));
        Assertions.assertEquals("anotherValue", PropertiesManager.getCustomProperty("another.property"));
    }

    @Test
    void testCanSetEmptyMapWithoutError() {
        PropertiesManager.setCustomProperties(Collections.emptyMap());
    }

    @Test
    void testGettingNonExistentCustomPropertyThrowsError() {
        Assertions.assertThrows(RuntimeException.class, () ->
                PropertiesManager.getCustomProperty(RandomStringUtils.secure().nextAlphabetic(35))
        );
    }

    // --- env-var override: getProperty ---

    @Test
    void envVarOverridesPropertyFileValue() {
        PropertiesManager.setCustomProperties(Map.of("test.property", "fromFile"));
        PropertiesManager.envReader = key -> key.equals("TEST_PROPERTY") ? "fromEnv" : null;
        Assertions.assertEquals("fromEnv", PropertiesManager.getCustomProperty("test.property"));
    }

    @Test
    void envVarAloneResolvesPropertyWithoutFileEntry() {
        PropertiesManager.envReader = key -> key.equals("ENV_ONLY_PROPERTY") ? "envOnly" : null;
        Assertions.assertEquals("envOnly", PropertiesManager.getCustomProperty("env.only.property"));
    }

    @Test
    void emptyEnvVarDoesNotOverridePropertyFileValue() {
        PropertiesManager.setCustomProperties(Map.of("test.property", "fromFile"));
        PropertiesManager.envReader = key -> "";
        Assertions.assertEquals("fromFile", PropertiesManager.getCustomProperty("test.property"));
    }

    @Test
    void nullEnvVarDoesNotOverridePropertyFileValue() {
        PropertiesManager.setCustomProperties(Map.of("test.property", "fromFile"));
        PropertiesManager.envReader = key -> null;
        Assertions.assertEquals("fromFile", PropertiesManager.getCustomProperty("test.property"));
    }

    // --- env-var override: hasProperty ---

    @Test
    void hasPropertyReturnsTrueWhenKeyInFile() {
        PropertiesManager.setCustomProperties(Map.of("has.property.test", "value"));
        Assertions.assertTrue(PropertiesManager.hasProperty("has.property.test"));
    }

    @Test
    void hasPropertyReturnsFalseWhenKeyAbsent() {
        Assertions.assertFalse(PropertiesManager.hasProperty(RandomStringUtils.secure().nextAlphabetic(35)));
    }

    @Test
    void hasPropertyReturnsTrueWhenEnvVarSetButNotInFile() {
        PropertiesManager.envReader = key -> key.equals("ENV_ONLY_PROPERTY") ? "envOnly" : null;
        Assertions.assertTrue(PropertiesManager.hasProperty("env.only.property"));
    }

    @Test
    void hasPropertyReturnsFalseWhenEnvVarIsEmpty() {
        PropertiesManager.envReader = key -> "";
        Assertions.assertFalse(PropertiesManager.hasProperty(RandomStringUtils.secure().nextAlphabetic(35)));
    }

    // --- toEnvKey mapping ---

    @Test
    void dotsConvertedToUnderscoresAndUppercasedInEnvLookup() {
        // key "a.b.c" should look up env var "A_B_C"
        PropertiesManager.envReader = key -> key.equals("A_B_C") ? "mapped" : null;
        PropertiesManager.setCustomProperties(Map.of("a.b.c", "fromFile"));
        Assertions.assertEquals("mapped", PropertiesManager.getCustomProperty("a.b.c"));
    }

    @Test
    void hyphensConvertedToUnderscoresInEnvLookup() {
        // key "a-b-c" should look up env var "A_B_C"
        PropertiesManager.envReader = key -> key.equals("A_B_C") ? "mapped" : null;
        PropertiesManager.setCustomProperties(Map.of("a-b-c", "fromFile"));
        Assertions.assertEquals("mapped", PropertiesManager.getCustomProperty("a-b-c"));
    }
}
