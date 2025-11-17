package com.atscale.java.dao;

import java.sql.Timestamp;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.atscale.java.utils.HashUtil;
import org.apache.commons.lang3.StringUtils;

@SuppressWarnings("unused")
public class QueryHistoryDto {
    private String queryName;
    private String service;
    private String queryLanguage;
    private String inboundText;
    private String inboundTextAsMd5Hash;
    private String outboundText;
    private String cubeName;
    private String projectId;
    private boolean aggregateUsed;
    private int numTimes;
    private Timestamp elapsedTimeInSeconds;
    private int avgResultSetSize;
    private String atscaleQueryId;

    public String getQueryName() {
        return queryName;
    }

    public void setQueryName(String queryName) {
        this.queryName = queryName;
    }

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

    public String getQueryLanguage() {
        return queryLanguage;
    }

    public void setQueryLanguage(String queryLanguage) {
        this.queryLanguage = queryLanguage;
    }

    public String getInboundText() {
        return inboundText;
    }

    public String getInboundTextAsMd5Hash() {
        return StringUtils.isEmpty(inboundTextAsMd5Hash)? HashUtil.TO_MD5(inboundText) : inboundTextAsMd5Hash;
    }

    public void setInboundText(String inboundText) {
        this.inboundText = inboundText;
        this.inboundTextAsMd5Hash = HashUtil.TO_MD5(inboundText);
    }

    public String getOutboundText() {
        return outboundText;
    }

    public void setOutboundText(String outboundText) {
        this.outboundText = outboundText;
    }

    public String getCubeName() {
        return cubeName;
    }

    public void setCubeName(String cubeName) {
        this.cubeName = cubeName;
    }

    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public boolean isAggregateUsed() {
        return aggregateUsed;
    }

    public void setAggregateUsed(boolean aggregateUsed) {
        this.aggregateUsed = aggregateUsed;
    }

    public int getNumTimes() {
        return numTimes;
    }

    public void setNumTimes(int numTimes) {
        this.numTimes = numTimes;
    }

    public Timestamp getElapsedTimeInSeconds() {
        return elapsedTimeInSeconds;
    }

    public void setElapsedTimeInSeconds(Timestamp elapsedTimeInSeconds) {
        this.elapsedTimeInSeconds = elapsedTimeInSeconds;
    }

    public int getAvgResultSetSize() {
        return avgResultSetSize;
    }

    public void setAvgResultSetSize(int avgResultSetSize) {
        this.avgResultSetSize = avgResultSetSize;
    }

    public String getAtscaleQueryId() {
        return atscaleQueryId;
    }
    public void setAtscaleQueryId(String atscaleQueryId) {
        this.atscaleQueryId = atscaleQueryId;
    }

    @Override
    public String toString() {
        return "QueryHistoryDto{" +
                "service='" + service + '\'' +
                ", queryLanguage='" + queryLanguage + '\'' +
                ", inboundText='" + inboundText + '\'' +
                ", inboundTextAsMd5Hash='" + inboundTextAsMd5Hash + '\'' +
                ", outboundText='" + outboundText + '\'' +
                ", cubeName='" + cubeName + '\'' +
                ", projectId='" + projectId + '\'' +
                ", aggregateUsed=" + aggregateUsed +
                ", numTimes=" + numTimes +
                ", elapsedTimeInSeconds=" + elapsedTimeInSeconds +
                ", avgResultSetSize=" + avgResultSetSize +
                ", atscaleQueryId='" + atscaleQueryId + '\'' +
                '}';
    }

    public String toJson() {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error converting QueryHistoryDto to JSON", e);
        }
    }

    public static QueryHistoryDto fromJson(String json) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.readValue(json, QueryHistoryDto.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error converting JSON to QueryHistoryDto", e);
        }
    }
}
