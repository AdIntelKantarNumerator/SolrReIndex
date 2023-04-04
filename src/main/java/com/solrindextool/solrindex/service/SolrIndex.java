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

    private String sourceUrl = "http://localhost:8983/solr/mynewtest6/";
    private String destinationUrl = "http://localhost:8983/solr/mynewtest13/";
    private int batchSize = 1000;
    private int threadCount = 20;
    private String startTime = "2022-03-16T10:56:10.342Z";
    private String endTime = "2022-03-17T10:56:10.342Z";

    public SolrIndex() {
    }

    public SolrIndex(String sourceUrl, String destinationUrl, int batchSize, int threadCount, String startTime, String endTime) {
        this.sourceUrl = sourceUrl;
        this.destinationUrl = destinationUrl;
        this.batchSize = batchSize;
        this.threadCount = threadCount;
        this.startTime = startTime;
        this.endTime = endTime;
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
            query.setQuery("*:*").addFilterQuery("timestamp:[" + startTime + " TO " + endTime + "]");;
            long totalCount = source.query(query).getResults().getNumFound();
            logger.info("Documents found: " + totalCount);

            final Stopwatch stopwatch = Stopwatch.createStarted();
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);

            // Start the transfer process
            Map<Integer,List> errorMap = start(source,destination,totalCount,query,executor);

            // Wait for all batches to finish processing
            executor.shutdown();
            while (!executor.isTerminated()) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    logger.error("Interrupted while waiting for batches to complete", e);
                }
            }
            if (!errorMap.isEmpty()){
                logger.info("Error processing batches: "+errorMap.toString());
            }
            final long duration = stopwatch.elapsed(TimeUnit.MILLISECONDS);
            logger.info("time spend: "+duration);

            // Close Solr clients
            source.close();
            destination.close();
            logger.info("Done!");

        } catch (SolrServerException | IOException e) {
            logger.error("Error starting SolrIndex", e);
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
                            if (!fieldName.equals("_version_")) {
                                inputDoc.addField(fieldName, doc.getFieldValue(fieldName));
                            }
                        });
                        return inputDoc;
                    }).collect(Collectors.toList());

                    destination.add(inputDocs);
                    destination.commit();
                    logger.info("Processed batch " + start + "-" + (start + rows - 1));

                } catch (SolrServerException | IOException e) {
                    logger.info("error processing job " + start + "-" + (start + rows - 1));
                    List<Integer> list = new ArrayList<>();
                    list.add(start);
                    list.add(rows);
                    errorMap.put(key[0],list);
                    key[0]++;
                }
            });
        }
        return errorMap;
    }
}

