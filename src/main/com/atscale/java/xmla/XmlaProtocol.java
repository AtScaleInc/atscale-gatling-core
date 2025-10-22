package com.atscale.java.xmla;

import com.atscale.java.utils.PropertiesManager;
import io.gatling.javaapi.http.*;
import static io.gatling.javaapi.http.HttpDsl.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

@SuppressWarnings("unused")
public class XmlaProtocol {
    private static final Logger LOGGER = LoggerFactory.getLogger(XmlaProtocol.class);

    public static HttpProtocolBuilder forXmla(String model) {
        String url = PropertiesManager.getAtScaleXmlaConnection(model);
        Integer maxConnections = PropertiesManager.getAtScaleXmlaMaxConnectionsPerHost();

        if (PropertiesManager.isContainerVersion(model)) {
            LOGGER.info("Configured for container version.  Auth token is part of the URL.");
            LOGGER.info("Configured for max connections per host: {}", maxConnections);
            return http.baseUrl(url)
                    .contentTypeHeader("text/xml; charset=UTF-8")
                    .acceptHeader("text/xml")
                    .maxConnectionsPerHost(maxConnections);
        } else {
            LOGGER.info("Configured for installer version.  Will obtain bearer auth token.");
            LOGGER.info("Configured for max connections per host: {}", maxConnections);
            String authUrl = PropertiesManager.getAtScaleXmlaAuthConnection(model);
            String tokenUserName = PropertiesManager.getAtScaleXmlaAuthUserName(model);
            String tokenPassword = PropertiesManager.getAtScaleXmlaAuthPassword(model);
            String bearerToken = getBearerToken(authUrl, tokenUserName, tokenPassword);
            LOGGER.info("Obtained bearer token for user {} and model {}.", tokenUserName, model);

            return http.baseUrl(url)
                    .contentTypeHeader("text/xml; charset=UTF-8")
                    .acceptHeader("text/xml")
                    .authorizationHeader(bearerToken)
                    .maxConnectionsPerHost(maxConnections);
        }
    }

    public static String getBearerToken(String urlString, String username, String password) {
        LOGGER.info("Getting bearer token from URL: {}", urlString);
        try {
            URL url = new URL(urlString);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            String auth = username + ":" + password;
            String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8));
            conn.setRequestProperty("Authorization", "Basic " + encodedAuth);

            int responseCode = conn.getResponseCode();
            if (responseCode != 200) {
                throw new RuntimeException("Failed : HTTP error code : " + responseCode);
            }

            try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                StringBuilder response = new StringBuilder();
                String line;
                while ((line = br.readLine()) != null) {
                    response.append(line);
                }
                return String.format("Bearer %s", response.toString().trim()); // assuming the token is the whole response body
            }
        } catch (IOException e) {
            LOGGER.error("Error while getting bearer token for {} from {}: {}", username, urlString, e.getMessage());
            throw new RuntimeException("Error while getting bearer token: " + e.getMessage(), e);
        }
    }
}
