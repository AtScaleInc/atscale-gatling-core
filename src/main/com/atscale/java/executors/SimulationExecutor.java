package com.atscale.java.executors;

import com.atscale.java.utils.AwsSecretsManager;
import com.atscale.java.utils.JsonUtil;
import com.atscale.java.utils.SecretsManager;

import java.io.File;
import java.nio.file.Paths;
import java.util.Map;

public abstract class SimulationExecutor {

    /** Factory method to create SecretsManager instance
     * Override this method to provide a different implementation of SecretsManager
     */
    protected SecretsManager createSecretsManager() {
        return new AwsSecretsManager();
    }

    /** Default implementation
     * Loads additional properties as a map of key value pairs from AWS Secrets Manager
     * params[0] - AWS region
     * params[1] - Secrets key
     *
     * The assumption is that we will pass this collection of additional properties
     * to our PropertiesFileReader to augment the properties read from the systems.properties file.
     *
     * It's passed via the MavenTaskDto additionalProperties field, which is a Map<String, String>
     * via a setter method:  setAdditionalProperties(Map<String, String> additionalProperties)
     * The simulations marshal all MavenTaksDto fields to the Gatling Simulations.
     *
     * We chose to store our secrets on AWS as a single key value pair.
     * A singleKey fetches a JSON string that represents a Map of key value pairs.  For instance:
     * Given a Map<String, String> storedSecrets = Map.of("dbUser", "admin", "dbPassword", "password123");
     * We store it in AWS Secrets Manager as:
     * {"mySecretKey": "{\"dbUser\":\"admin\",\"dbPassword\":\"password123\"}"}
     * where "mySecretKey" is the key used to fetch the data from AWS Secrets Manager.
     * JsonUtil has methods to convert a Map to a JSON string, and a JSON string back to a Map.
     *
     * This has been written in a very generic fashion so that other implementations
     * of SecretsManager such as for other cloud providers can be created and used in the future.
     *
     * Alternately, build a map any way you wish, perhaps from environment variables
     * and return it.
     */
    public Map<String, String> additionalProperties(String... params) {
        String region = params[0];
        String secretsKey = params[1];
        SecretsManager secretsManager = createSecretsManager();
        Map<String, String> awsSecrets = secretsManager.loadSecrets(region, secretsKey);
        return awsSecrets;
    }

    protected String getMavenWrapperScript() {
        String osName = System.getProperty("os.name").toLowerCase();
        return osName.contains("win") ?  "mvnw.cmd" : "./mvnw";
    }

    protected String getApplicationDirectory() {
        try {
            String path = Paths.get(System.getProperty("user.dir")).toString();
            File file = new File(path);
            if (! file.isDirectory()){
                throw new RuntimeException("Resolved to path, but is not a valid directory: " + path);
            }
            return path;
        } catch (Exception e) {
            throw new RuntimeException("Unable to determine application directory", e);
        }
    }

}
