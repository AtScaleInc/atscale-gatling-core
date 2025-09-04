package com.atscale.java.jdbc;

import com.zaxxer.hikari.HikariConfig;
import org.galaxio.gatling.javaapi.protocol.JdbcProtocolBuilder;
import static org.galaxio.gatling.javaapi.JdbcDsl.DB;
import com.atscale.java.utils.PropertiesFileReader;
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
        String url = getJdbcUrl(model);
        String userName = PropertiesFileReader.getAtScaleJdbcUserName(model);
        String password = PropertiesFileReader.getAtScaleJdbcPassword(model);
        int maxPool = PropertiesFileReader.getAtScaleJdbcMaxPoolSize(model);

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(url);
        hikariConfig.setUsername(userName);
        hikariConfig.setPassword(password);
        hikariConfig.setMaximumPoolSize(maxPool);
        hikariConfig.setConnectionTestQuery("SELECT 1");

        return DB().hikariConfig(hikariConfig);
    }

    private static String getJdbcUrl(String model) {
        String url = PropertiesFileReader.getAtScaleJdbcConnection(model);
        // Split the URL to encode only the database name
        int idx = url.indexOf('/', "jdbc:hive2://".length());
        if (idx > 0 && idx + 1 < url.length()) {
            String base = url.substring(0, idx + 1);
            String dbName = url.substring(idx + 1);
            dbName = URLEncoder.encode(dbName, StandardCharsets.UTF_8).replace("+", "%20");
            url = base + dbName;
        }
        return url;
    }
}
