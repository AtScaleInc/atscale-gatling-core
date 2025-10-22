package com.atscale.java.utils;

import java.util.Map;

public interface SecretsManager {
    Map<String, String> loadSecrets(String... params);
}
