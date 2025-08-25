package com.atscale.java.jdbc;

import org.galaxio.gatling.javaapi.protocol.JdbcProtocolBuilder;
import static org.galaxio.gatling.javaapi.JdbcDsl.DB;
import com.atscale.java.utils.PropertiesFileReader;

@SuppressWarnings("unused")
public class JdbcProtocol {
    /**
     * Creates a JdbcProtocolBuilder for the AtScale JDBC connection.
     *
     * @return JdbcProtocolBuilder configured with AtScale JDBC connection details.
     */

    public static JdbcProtocolBuilder forDatabase(String model) {
        String url = PropertiesFileReader.getAtScaleJdbcConnection(model);
        String userName = PropertiesFileReader.getAtScaleJdbcUserName(model);
        String password = PropertiesFileReader.getAtScaleJdbcPassword(model);
        int maxPool = PropertiesFileReader.getAtScaleJdbcMaxPoolSize(model);
        return DB()
                .url(url)
                .username(userName)
                .password(password)
                .maximumPoolSize(maxPool)
                .protocolBuilder();
    }
}
