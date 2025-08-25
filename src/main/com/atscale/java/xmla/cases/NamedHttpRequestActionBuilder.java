package com.atscale.java.xmla.cases;

import io.gatling.javaapi.http.HttpRequestActionBuilder;

public class NamedHttpRequestActionBuilder {
    public final HttpRequestActionBuilder builder;
    public final String queryName;

    public NamedHttpRequestActionBuilder(HttpRequestActionBuilder builder, String queryName) {
        this.builder = builder;
        this.queryName = queryName;
    }
}
