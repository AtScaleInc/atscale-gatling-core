package com.atscale.java.utils;

import java.util.Map;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

public class AwsSecretsManager implements SecretsManager{

    public Map<String, String> loadSecrets(String... params) {
        SecretsManagerClient client = null;

        try {
            client = SecretsManagerClient.builder()
                    .region(Region.of(params[0]))
                    .build();

            GetSecretValueRequest getSecretValueRequest = GetSecretValueRequest.builder()
                    .secretId(params[1])
                    .build();

            GetSecretValueResponse getSecretValueResponse = client.getSecretValue(getSecretValueRequest);

            // Convert the aws response to a map
            // AWS sends back a map with the key and the value.  In our case the value is a Java Map in the
            // form of a JSON string.  Use the key to get the value, convert it to a map of key value pairs
            // and return it to the caller.
            Map<String, String> awsSecrets = JsonUtil.asMap(getSecretValueResponse.secretString());
            return JsonUtil.asMap(awsSecrets.get(params[1]));
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }
}
