package com.atscale.java.jdbc;

import com.zaxxer.hikari.HikariConfig;
import org.galaxio.gatling.javaapi.protocol.JdbcProtocolBuilder;
import static org.galaxio.gatling.javaapi.JdbcDsl.DB;
import com.atscale.java.utils.PropertiesFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

@SuppressWarnings("unused")
public class JdbcProtocol {
    /**
     * Creates a JdbcProtocolBuilder for the AtScale JDBC connection.
     *
     * @return JdbcProtocolBuilder configured with AtScale JDBC connection details.
     */

    public static JdbcProtocolBuilder forDatabase(String model) {
        Logger LOGGER = LoggerFactory.getLogger(JdbcProtocol.class);
        String url = getJdbcUrl(model);
        String userName = PropertiesFileReader.getAtScaleJdbcUserName(model);
        String password = PropertiesFileReader.getAtScaleJdbcPassword(model);
        int maxPool = PropertiesFileReader.getAtScaleJdbcMaxPoolSize(model);
        String useAggregates = PropertiesFileReader.getJdbcUseAggregates();
        String useLocalCache = PropertiesFileReader.getJdbcUseLocalCache();
        String createAggregates = PropertiesFileReader.getJdbcGenerateAggregates();
        String initSql = String.format("set use_local_cache = %s; set create_aggregates = %s; set use_aggregates = %s", useLocalCache, createAggregates, useAggregates);

        LOGGER.info("Initializing each connection with {}", initSql);

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(url);
        hikariConfig.setUsername(userName);
        hikariConfig.setPassword(password);
        hikariConfig.setMaximumPoolSize(maxPool);
        hikariConfig.setConnectionInitSql(initSql);
        hikariConfig.setConnectionTestQuery("SELECT 1");

        return DB().hikariConfig(hikariConfig);
    }

    private static String getJdbcUrl(String model) {
        String url = PropertiesFileReader.getAtScaleJdbcConnection(model);
        // Split the URL to encode only the database name for Hive
        if(url.toLowerCase().contains("hive")) {
            int idx = url.indexOf('/', "jdbc:hive2://".length());
            if (idx > 0 && idx + 1 < url.length()) {
                String base = url.substring(0, idx + 1);
                String dbName = url.substring(idx + 1);
                dbName = URLEncoder.encode(dbName, StandardCharsets.UTF_8).replace("+", "%20");
                url = base + dbName;
            }
        }
        return url;
    }
}
