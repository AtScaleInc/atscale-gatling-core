package com.atscale.java.utils;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.mockito.Mockito;

public class SecretsManagerTest {

    @Test
    public void testAbleToExtractSimplePair() {
        AwsSecretsManager spy = org.mockito.Mockito.spy(new AwsSecretsManager());
        Map<String, String> testSecrets = Map.of("test.secret", "secretValue");

        org.mockito.Mockito.doReturn(JsonUtil.asJson(testSecrets)).when(spy).callAws(Mockito.anyString(), Mockito.anyString());

        Map<String, String> loadedSecrets = spy.loadSecrets("us-east-1", "topsecret");
        Assertions.assertNotNull(loadedSecrets);
        Assertions.assertEquals(testSecrets, loadedSecrets);
    }

    @Test
    public void testAbleToExtractPairs() {
        AwsSecretsManager spy = org.mockito.Mockito.spy(new AwsSecretsManager());
        Map<String, String> testSecrets = randomSizedMap();

        String region = "us-east-1";
        String secretName = "topsecret";

        org.mockito.Mockito.doReturn(JsonUtil.asJson(testSecrets)).when(spy).callAws(region, secretName);

        Map<String, String> loadedSecrets = spy.loadSecrets(region, secretName);
        Assertions.assertNotNull(loadedSecrets);
        Assertions.assertEquals(testSecrets, loadedSecrets);
    }

    @Test
    public void testAbleToExtractMapAsFlatMap() {
        AwsSecretsManager spy = org.mockito.Mockito.spy(new AwsSecretsManager());
        Map<String, String> testSecrets = randomSizedMap();
        String testSecretsJson = JsonUtil.asJson(testSecrets);
        Map<String, String> mappedTestSecrets = Map.of("key1", testSecretsJson);

        String region = "us-east-1";
        String secretName = "topsecret";

        org.mockito.Mockito.doReturn(JsonUtil.asJson(mappedTestSecrets)).when(spy).callAws(region, secretName);

        Map<String, String> loadedSecrets = spy.loadSecrets(region, secretName);

        Assertions.assertNotNull(loadedSecrets);
        Assertions.assertEquals(testSecrets, loadedSecrets);
    }

    @Test
    public void testAbleToExtractMapsAsFlatMap() {
        AwsSecretsManager spy = org.mockito.Mockito.spy(new AwsSecretsManager());
        Map<String, String> mappedTestSecrets = new HashMap<>();
        Map<String, String> flattenedTestSecrets = new HashMap<>();

        String[] mapKeys = {"key1", "key2", "key3"};
        for(String mapKey: mapKeys) {
            Map<String, String> testSecrets = randomSizedMap();
            String testSecretsJson = JsonUtil.asJson(testSecrets);
            mappedTestSecrets.put(mapKey, testSecretsJson);
            flattenedTestSecrets.putAll(testSecrets);
        }

        String region = "us-east-1";
        String secretName = "topsecret";

        org.mockito.Mockito.doReturn(JsonUtil.asJson(mappedTestSecrets)).when(spy).callAws(region, secretName);

        Map<String, String> loadedSecrets = spy.loadSecrets(region, secretName);

        Assertions.assertNotNull(loadedSecrets);
        Assertions.assertEquals(flattenedTestSecrets, loadedSecrets);
    }

    @Test
    public void testAbleToExtractComplexDataStructure() {
        AwsSecretsManager spy = org.mockito.Mockito.spy(new AwsSecretsManager());
        Map<String, String> mappedTestSecrets = new HashMap<>();
        Map<String, String> flattenedTestSecrets = new HashMap<>();

        String[] mapKeys = {"key1", "key2", "key3"};
        for(String mapKey: mapKeys) {
            Map<String, String> testSecrets = randomSizedMap();
            String testSecretsJson = JsonUtil.asJson(testSecrets);
            mappedTestSecrets.put(mapKey, testSecretsJson);
            flattenedTestSecrets.putAll(testSecrets);
        }

        Map<String, String> kvPairs = Map.of("simpleKey1", "simpleValue1", "simpleKey2", "simpleValue2");
        mappedTestSecrets.putAll(kvPairs);
        flattenedTestSecrets.putAll(kvPairs);

        String region = "us-east-1";
        String secretName = "topsecret";

        org.mockito.Mockito.doReturn(JsonUtil.asJson(mappedTestSecrets)).when(spy).callAws(region, secretName);

        Map<String, String> loadedSecrets = spy.loadSecrets(region, secretName);

        Assertions.assertNotNull(loadedSecrets);
        Assertions.assertEquals(flattenedTestSecrets, loadedSecrets);
    }

    protected Map<String, String> randomSizedMap() {
        Map<String, String> testMap = new HashMap<>();
        String seed = RandomStringUtils.secure().nextAlphabetic(5);
        int numSecrets = new Random().nextInt(1, 5);
        Map<String, String> testSecrets = new HashMap<>();
        for (int i = 0; i < numSecrets; i++) {
            testSecrets.put(String.format("secret.key.%s.%s", seed, i), String.format("secretValue.%s.%s", seed, i));
        }
        return testSecrets;
    }
}
