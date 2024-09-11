package com.example.bigquery;


import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

@SpringBootApplication
public class BigqueryApplication {

//    public static void main(String[] args) {
//        SpringApplication.run(BigqueryApplication.class, args);
//    }

    private static final String PROJECT_ID = "refined-outlet-433207-c3";

    private static final String DATASET_ID = "demo";
    private static final String TABLE_ID = "transaction";

    public static void main(String[] args) throws Exception {

//  Config BigQuery Credentials and Project Id
        Resource resource = new ClassPathResource(
                "/data/refined-outlet-433207-c3-29cf3faa0c72.json");
        GoogleCredentials credentials = GoogleCredentials.fromStream(resource.getInputStream());

        BigQuery bigQuery = BigQueryOptions.newBuilder()
                .setCredentials(credentials)
                .setProjectId(PROJECT_ID)
                .build().getService();

//  Transfer data to BigQuery

//      Mock data query from Database
        Transaction transaction1 = Transaction.builder()
                .id(7L)
                .fromAddress("A")
                .toAddress("B")
                .currencySN("BTC")
                .balance(new BigDecimal(1))
                .createdDate(Instant.now().minus(1, ChronoUnit.DAYS))
                .build();
        Transaction transaction2 = Transaction.builder()
                .id(8L)
                .fromAddress("A")
                .toAddress("B")
                .currencySN("BTC")
                .balance(new BigDecimal(1))
                .createdDate(Instant.now().plusSeconds(2).minus(1, ChronoUnit.DAYS))
                .build();
        List <Transaction> transactions = List.of(transaction1, transaction2);

//      Convert Entity to Big Query Objects
        Queue <InsertAllRequest.RowToInsert> rows = new ConcurrentLinkedQueue <>();
        transactions.parallelStream()
                .forEach(transaction ->
                {
                    InsertAllRequest.RowToInsert row = InsertAllRequest.RowToInsert
                            .of(convertToRowBigQuery(transaction));
                    rows.add(row);
                });

//      Insert data to Big Query
        TableId tableId = TableId.of(DATASET_ID, TABLE_ID);
        InsertAllRequest insertRequest = InsertAllRequest
                .newBuilder(tableId)
                .setRows(rows)
                .build();
        InsertAllResponse response = bigQuery.insertAll(insertRequest);

//      Check and print if has errors
        if (response.hasErrors()) {
            for (Map.Entry <Long, List <BigQueryError>> error : response.getInsertErrors().entrySet()) {
                System.out.println(error.getValue().toString());
            }
        } else {

//      Query data from Big Query
//          Create query
            final String SELECT_DEMO_SQL =
                    "SELECT * FROM `demo.transaction` " +
                            "WHERE TIMESTAMP_TRUNC(created_date, DAY) = TIMESTAMP(\"2024-09-07\")";

//          Create Big Query job
            QueryJobConfiguration queryJobConfig =
                    QueryJobConfiguration.newBuilder(SELECT_DEMO_SQL).build();
            Job queryJob = bigQuery.create(JobInfo.newBuilder(queryJobConfig).build());

//          Execute job (request to Big Query)
            queryJob = queryJob.waitFor();

//          Check and print if has errors
            if (queryJob.getStatus().getError() != null) {
                System.out.println(queryJob.getStatus().getError().getMessage());
            }
            else {
//          If has no error, print result
                TableResult result = queryJob.getQueryResults();
                for (FieldValueList row : result.iterateAll()) {
                    String record = row.toString();
                    System.out.println(record);
                }
            }
        }
    }

    private static Map<String, Object> convertToRowBigQuery(Transaction transaction) {
        Map<String, Object> row = new HashMap <>();
        row.put("id", transaction.getId());
        row.put("from_address", transaction.getFromAddress());
        row.put("to_address", transaction.getToAddress());
        row.put("currency_sn", transaction.getCurrencySN());
        row.put("balance", transaction.getBalance());
        row.put("created_date", transaction.getCreatedDate().getEpochSecond());
        return row;
    }

}


//SELECT
//        t.from_address,
//        TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY), DAY) AS created_day,
//        SUM(t.balance) AS total_balance
//        FROM
//        `refined-outlet-433207-c3.demo.transaction` AS t
//        WHERE
//        TIMESTAMP_TRUNC(t.created_date, DAY) = TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY), DAY)
//        GROUP BY
//        t.from_address;