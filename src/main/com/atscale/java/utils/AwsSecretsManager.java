package com.atscale.java.utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections4.Get;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

public class AwsSecretsManager implements SecretsManager{

    public Map<String, String> loadSecrets(String... params) {
        return processAwsSecrets(callAws(params));
    }

    public String callAws(String... params) {
        SecretsManagerClient client = null;

        try {
            client = SecretsManagerClient.builder()
                    .region(Region.of(params[0]))
                    .build();

            GetSecretValueRequest getSecretValueRequest = GetSecretValueRequest.builder()
                    .secretId(params[1])
                    .build();

            GetSecretValueResponse getSecretValueResponse = client.getSecretValue(getSecretValueRequest);

            return getSecretValueResponse.secretString();
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    protected Map<String, String> processAwsSecrets(String secretString) {
        Map<String, String> flattenedSecrets = new HashMap<>();

        Map<String, String> awsSecrets = JsonUtil.asMap(secretString);
        for(String secretKey: awsSecrets.keySet()) {
            flattenedSecrets.put(secretKey, awsSecrets.get(secretKey));

        }
        return flattenedSecrets;
    }
}
