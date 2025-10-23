package com.atscale.java.utils;

import java.util.Map;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

public class AwsSecretsManager implements SecretsManager{

    public Map<String, String> loadSecrets(String... params) {
        return processAwsSecrets(callAws(params));
    }

    public String callAws(String... params) {
        try (SecretsManagerClient client = SecretsManagerClient.builder().region(Region.of(params[0])).build()) {
            GetSecretValueRequest getSecretValueRequest = GetSecretValueRequest.builder()
                    .secretId(params[1])
                    .build();

            GetSecretValueResponse getSecretValueResponse = client.getSecretValue(getSecretValueRequest);

            return getSecretValueResponse.secretString();
        }
    }

    protected Map<String, String> processAwsSecrets(String secretString) {
        return JsonUtil.asMap(secretString);
    }
}
