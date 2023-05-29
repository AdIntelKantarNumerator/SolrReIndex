package com.solrindextool.solrindex.service;

import com.google.common.base.Stopwatch;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class SolrIndex {
    Logger logger = LoggerFactory.getLogger(SolrIndex.class);

    private final String sourceUrl;
    private final String destinationUrl;
    private final int batchSize;
    private final int threadCount;
    private String startTime;
    private String endTime;
    private String customQuery;
    private boolean isCustomQuery;

    public SolrIndex(String sourceURL, String destinationURL, int batchSize, int threadCount,
                     String startTime, String endTime, boolean isCustomQuery) {
        this.sourceUrl = sourceURL;
        this.destinationUrl = destinationURL;
        this.batchSize = batchSize;
        this.threadCount = threadCount;
        this.startTime = startTime;
        this.endTime = endTime;
        this.isCustomQuery = isCustomQuery;
    }

    public SolrIndex(String sourceURL, String destinationURL, int batchSize, int threadCount, boolean isCustomQuery, String customQuery) {
        this.sourceUrl = sourceURL;
        this.destinationUrl = destinationURL;
        this.batchSize = batchSize;
        this.threadCount = threadCount;
        this.isCustomQuery = isCustomQuery;
        this.customQuery = customQuery;
    }

    public void copy() {

        try {
            logger.info("Application started");

            // Initialize Solr clients for source and destination
            SolrClient source = new HttpSolrClient.Builder(sourceUrl).build();
            SolrClient destination = new HttpSolrClient.Builder(destinationUrl).build();
            logger.info("Solr clients initialized for source and destination");

            // Query Solr for documents to copy
            SolrQuery query = new SolrQuery();
            if (isCustomQuery){
                String url = customQuery;
                // Get the query and filter query parameters from the URL
                String queryString = url.substring(url.indexOf("?") + 1);
                String[] queryParams = queryString.split("&");

                // Create a SolrQuery object and set the query and filter query parameters
                for (String queryParam : queryParams) {
                    String[] paramParts = queryParam.split("=");
                    String paramName = paramParts[0];
                    String paramValue = paramParts[1];
                    if (paramName.equals("q")) {
                        query.setQuery(paramValue);
                    } else if (paramName.equals("fq")) {
                        query.addFilterQuery(paramValue);
                    }
                }
            }
            else {
                query.setQuery("*:*").addFilterQuery("first_run_date:[" + startTime + " TO " + endTime + "]");
            }

            long totalCount = source.query(query).getResults().getNumFound();
            logger.info("Documents found: " + totalCount);

            final Stopwatch stopwatch = Stopwatch.createStarted();
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);

            // Start the transfer process
            Map<Integer,List> errorMap = start(source,destination,totalCount,query,executor);

            if (!errorMap.isEmpty()){
                logger.info("Error processing batches: "+ errorMap);
            }
            final long duration = stopwatch.elapsed(TimeUnit.MILLISECONDS);
            logger.info("time spend: "+duration);

            // Close Solr clients
            source.close();
            destination.close();
            logger.info("Done!");

        } catch (SolrServerException | IOException e) {
            logger.error("Error starting SolrIndex {}", e.getMessage());
        }
    }

    public Map<Integer, List> start(SolrClient source, SolrClient destination, long totalCount, SolrQuery query, ExecutorService executor){
        Map<Integer,List> errorMap = new HashMap<>();
        final int[] key = {1};

        for (int i = 0; i < totalCount; i += batchSize) {
            int start = i;
            int rows = Math.min(batchSize, (int) (totalCount - start));
            logger.info("starting new batch " + start + "-" + (start + rows - 1));

            executor.execute(() -> {
                // Query Solr for a batch of documents
                SolrQuery batchQuery = query.getCopy();
                batchQuery.setStart(start);
                batchQuery.setRows(rows);
                SolrDocumentList documents = null;
                try {
                    documents = source.query(batchQuery).getResults();
                    logger.info("successfully queried batch " + start + "-" + (start + rows - 1));

                    // Convert SolrDocumentList to List<SolrInputDocument>
                    List<SolrInputDocument> inputDocs = documents.parallelStream().map(doc -> {
                        SolrInputDocument inputDoc = new SolrInputDocument();
                        doc.getFieldNames().forEach(fieldName -> {
                            if (!fieldName.equals("_version_") && !fieldName.equals("timestamp")) {
                                inputDoc.addField(fieldName, doc.getFieldValue(fieldName));
                            }
                        });
                        return inputDoc;
                    }).collect(Collectors.toList());

                    destination.add(inputDocs);
                    destination.commit();
                    logger.info("Processed batch " + start + "-" + (start + rows - 1));

                } catch (SolrServerException | IOException e) {
                    logger.info("error processing job " + start + "-" + (start + rows - 1) + " " + e.getMessage());
                    List<Integer> list = new ArrayList<>();
                    list.add(start);
                    list.add(rows);
                    errorMap.put(key[0],list);
                    key[0]++;
                }
            });
        }

        // Wait for all batches to finish processing
        executor.shutdown();
        while (!executor.isTerminated()) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                logger.error("Interrupted while waiting for batches to complete", e);
            }
        }
        return errorMap;
    }
}

