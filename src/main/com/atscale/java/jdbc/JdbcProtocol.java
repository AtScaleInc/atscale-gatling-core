package com.atscale.java.jdbc;

import com.atscale.java.utils.PropertiesManager;
import com.zaxxer.hikari.HikariConfig;
import org.apache.commons.lang3.StringUtils;
import org.galaxio.gatling.javaapi.protocol.JdbcProtocolBuilder;
import static org.galaxio.gatling.javaapi.JdbcDsl.DB;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

@SuppressWarnings("unused")
public class JdbcProtocol {
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcProtocol.class);
    /**
     * Creates a JdbcProtocolBuilder for the AtScale JDBC connection.
     *
     * @return JdbcProtocolBuilder configured with AtScale JDBC connection details.
     */

    public static JdbcProtocolBuilder forDatabase(String model) {
        String url = getJdbcUrl(model);
        String userName = PropertiesManager.getAtScaleJdbcUserName(model);
        String password = PropertiesManager.getAtScaleJdbcPassword(model);
        int maxPool = PropertiesManager.getAtScaleJdbcMaxPoolSize(model);
        String useAggregates = PropertiesManager.getJdbcUseAggregates();
        String useLocalCache = PropertiesManager.getJdbcUseLocalCache();
        String createAggregates = PropertiesManager.getJdbcGenerateAggregates();

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(url);
        hikariConfig.setUsername(userName);
        hikariConfig.setPassword(password);
        hikariConfig.setMaximumPoolSize(maxPool);
        if(StringUtils.isNotEmpty(url) && url.contains("! jdbc:snowflake")) {
            // for AtScale Set Connection init SQL to specified cache and aggregate settings
            String initSql = String.format("set use_local_cache = %s; set create_aggregates = %s; set use_aggregates = %s", useLocalCache, createAggregates, useAggregates);
            LOGGER.debug("Initializing each connection with {}", initSql);
            hikariConfig.setConnectionInitSql(initSql);
        }
        hikariConfig.setConnectionTestQuery("SELECT 1");
        hikariConfig.setConnectionTimeout(PropertiesManager.getAtScaleJdbcConnectionTimeoutMs());
        int socketTimeout = PropertiesManager.getAtScaleJdbcSocketTimeoutSeconds();
        hikariConfig.addDataSourceProperty("socketTimeout", String.valueOf(socketTimeout));
        hikariConfig.addDataSourceProperty("loginTimeout", String.valueOf(socketTimeout));

        return DB().hikariConfig(hikariConfig);
    }

    private static String getJdbcUrl(String model) {
        String url = PropertiesManager.getAtScaleJdbcConnection(model);
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
