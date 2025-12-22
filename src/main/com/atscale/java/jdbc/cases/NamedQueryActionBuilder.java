package com.atscale.java.jdbc.cases;

import org.galaxio.gatling.javaapi.actions.QueryActionBuilder;

public class NamedQueryActionBuilder {
    public final QueryActionBuilder builder;
    public final String queryName;
    public final String inboundTextAsHash;
    public final String atscaleQueryId;
    public final String inboundText;

    public NamedQueryActionBuilder(QueryActionBuilder builder, String queryName, String inboundTextAsHash, String atscaleQueryId, String inboundText) {
        this.builder = builder;
        this.queryName = queryName;
        this.inboundTextAsHash = inboundTextAsHash;
        this.atscaleQueryId = atscaleQueryId;
        this.inboundText = inboundText;
    }
}
