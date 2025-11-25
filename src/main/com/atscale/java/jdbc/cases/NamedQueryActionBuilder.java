package com.atscale.java.jdbc.cases;

import io.gatling.core.session.Session;
import org.galaxio.gatling.javaapi.actions.QueryActionBuilder;

public class NamedQueryActionBuilder {
    public final QueryActionBuilder builder;
    public final String queryName;
    public final String inboundTextAsHash;
    public final String atscaleQueryId;
    public Session session;

    public NamedQueryActionBuilder(QueryActionBuilder builder, String queryName, String inboundTextAsHash, String atscaleQueryId) {
        this.builder = builder;
        this.queryName = queryName;
        this.inboundTextAsHash = inboundTextAsHash;
        this.atscaleQueryId = atscaleQueryId;
    }
}
