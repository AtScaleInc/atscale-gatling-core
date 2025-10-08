package com.atscale.java.jdbc.cases;

import io.gatling.core.session.Session;
import org.galaxio.gatling.javaapi.actions.QueryActionBuilder;

public class NamedQueryActionBuilder {
    public final QueryActionBuilder builder;
    public final String queryName;
    public final String inboundTextAsMd5Hash;
    public Session session;

    public NamedQueryActionBuilder(QueryActionBuilder builder, String queryName, String inboundTextAsMd5Hash) {
        this.builder = builder;
        this.queryName = queryName;
        this.inboundTextAsMd5Hash = inboundTextAsMd5Hash;
    }
}
