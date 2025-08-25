package com.atscale.java.jdbc.cases;

import org.galaxio.gatling.javaapi.actions.QueryActionBuilder;

public class NamedQueryActionBuilder {
    public final QueryActionBuilder builder;
    public final String queryName;

    public NamedQueryActionBuilder(QueryActionBuilder builder, String queryName) {
        this.builder = builder;
        this.queryName = queryName;
    }
}
