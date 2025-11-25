package com.atscale.java.dao;

import org.apache.commons.lang3.RandomStringUtils;
import com.atscale.java.dao.AtScalePostgresDao.QueryLanguage;

import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.time.LocalDateTime;

public class TestDataDto {
    protected Query queryData;
    protected SubQuery subQueryData;
    protected QueryResults queryResultsData;
    protected QueriesPlanned queriesPlannedData;

     public TestDataDto() {
         UUID queryId = UUID.randomUUID();
         queryData = new Query(queryId);
         queriesPlannedData = new QueriesPlanned(queryData);
         subQueryData = new SubQuery(queryData);
         queryResultsData = new QueryResults(queryData);

     }


    protected class Query {
        UUID queryId;
        String userId;
        String remoteAddress;
        String queryLanguage;
        LocalDateTime received;
        String query_text;
        String service;
        UUID session_id;
        String subject_id;

        protected Query(UUID queryId) {
            this.queryId = queryId;
            this.userId = "test_user";
            this.remoteAddress = "0.0.0.0";
            this.queryLanguage = QueryLanguage.SQL.getValue();
            this.received = LocalDateTime.now();
            this.query_text = String.format("SELECT %s", RandomStringUtils.insecure().nextAscii(35));
            this.service = "user-query";
            this.session_id = UUID.randomUUID();
            this.subject_id = "test_subject";
        }
    }

    protected class QueriesPlanned {
        UUID queryId;
        LocalDateTime planningStarted;
        LocalDateTime planningCompleted;
        String projectID;
        String cubeId;
        String cubeName;
        String logicalPlan;
        String physicalPlan;

        protected QueriesPlanned(Query queryData) {
            this.queryId = queryData.queryId;
            this.planningStarted = queryData.received.plus(10, ChronoUnit.MILLIS);
            this.planningCompleted = queryData.received.plus(20, ChronoUnit.MILLIS);
            this.projectID = String.format("project_%s", RandomStringUtils.insecure().nextAscii(5));
            this.cubeId = UUID.randomUUID().toString();
            this.cubeName = "Sales_Model";
            this.logicalPlan = "logical plan";
            this.physicalPlan = "physical plan";
        }
    }

    protected class SubQuery {
        UUID subQueryId;
        UUID queryId;
        LocalDateTime started;
        String queryLanguage;
        String connectionId;
        String subqueryText;
        String dialect;
        String queryPart;
        boolean isCanary;
        UUID subGroupId;

        protected SubQuery(Query queryData) {
            this.subQueryId = UUID.randomUUID();
            this.queryId = queryData.queryId;
            this.started = queryData.received.plus(120, ChronoUnit.MILLIS);
            this.queryLanguage = queryData.queryLanguage;
            this.connectionId = String.format("conn_%s", RandomStringUtils.insecure().nextAscii(10));
            this.subqueryText = String.format("SELECT %s", RandomStringUtils.insecure().nextAscii(35));
            this.dialect = "DatabricksSQL-1.0";
            this.queryPart = "main";
            this.isCanary = false;
            this.subGroupId = UUID.randomUUID();
        }
    }

     protected class QueryResults {
         UUID queryId;
         LocalDateTime finished;
         boolean succeeded;
         int resultSetSize;
         String failureMessage;

         protected QueryResults(Query queryData) {
             this.queryId = queryData.queryId;
             this.finished = queriesPlannedData.planningCompleted.plus(88, ChronoUnit.MILLIS);
             this.succeeded = true;
             this.resultSetSize = (int) (Math.random() * 1000);
             this.failureMessage = null;
         }
     }
}
