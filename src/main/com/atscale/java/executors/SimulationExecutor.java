package com.atscale.java.executors;

import com.atscale.java.utils.AwsSecretsManager;
import com.atscale.java.utils.JsonUtil;
import com.atscale.java.utils.SecretsManager;

import java.io.File;
import java.nio.file.Paths;
import java.util.Map;

/**
 * Base executor for simulations.
 *
 * Provides a factory for {@link SecretsManager} and utilities to load additional properties,
 * resolve the Maven wrapper script, and determine the application directory.
 */
public abstract class SimulationExecutor {

    /**
     * Factory method to create a {@link SecretsManager} instance.
     * Override this method to provide a different implementation of {@link SecretsManager}.
     *
     * @return a new {@link SecretsManager} instance
     */
    protected SecretsManager createSecretsManager() {
        return new AwsSecretsManager();
    }

    /**
     * Default implementation.
     * Loads additional properties as a map of key/value pairs from a secrets store.
     *
     * The method accepts varargs in {@code params} and expects:
     * <ul>
     *   <li>{@code params[0]} - AWS region</li>
     *   <li>{@code params[1]} - Secrets key</li>
     * </ul>
     *
     * The implementation uses {@link SecretsManager#loadSecrets(String...)} to fetch a map
     * where the secret value is the JSON string representation of a {@code Map<String,String>}
     * (see {@link JsonUtil} for marshalling/unmarshalling).
     *
     * @param params varargs where {@code params[0]} is the region and {@code params[1]} is the secrets key
     * @return a map of additional properties loaded from the secrets store
     */
    public Map<String, String> additionalProperties(String... params) {
        String region = params[0];
        String secretsKey = params[1];
        SecretsManager secretsManager = createSecretsManager();
        return secretsManager.loadSecrets(region, secretsKey);
    }

    /**
     * Returns the platform-appropriate Maven wrapper script name.
     *
     * @return {@code "mvnw.cmd"} on Windows, otherwise {@code "./mvnw"}
     */
    protected String getMavenWrapperScript() {
        String osName = System.getProperty("os.name").toLowerCase();
        return osName.contains("win") ?  "mvnw.cmd" : "./mvnw";
    }

    /**
     * Resolves the application directory from the current working directory.
     *
     * @return the absolute path to the application directory
     * @throws RuntimeException if the resolved path is not a directory or cannot be determined
     */
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
