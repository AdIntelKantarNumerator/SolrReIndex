package com.solrindextool;

import com.google.gson.Gson;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.FileNotFoundException;
import java.io.FileReader;

@SpringBootApplication
public class Start {
    static boolean custom;
    static String customQuery;
    static String sourcePort;
    static String destinationPort;
    static int batchSize;
    static int threadCount;
    static String startTime;
    static String endTime;
    static boolean collectionRunner;
    static String[] collections;

    public static void main(String[] args) throws FileNotFoundException {
        SpringApplication.run(Start.class, args);
        Gson gson = new Gson();
        Config config;

        try {
            config = gson.fromJson(new FileReader("config.json"), Config.class);
            custom = config.custom;
            collectionRunner = config.collectionRunner;
            customQuery=config.customQuery;
            sourcePort= config.sourcePort;
            destinationPort= config.destinationPort;
            batchSize=config.batchSize;
            threadCount=config.threadCount;
            startTime = config.startTimestamp;
            endTime = config.endTimestamp;
            collections = config.collections;
        } catch (Exception e){
            System.out.println("config file not found");
            throw e;
        }

        Runner runner;
        if (custom){
            runner = new Runner(sourcePort, destinationPort, batchSize, threadCount, true, customQuery);
            runner.copy();
            System.exit(0);
        } else if (collectionRunner) {
            runner = new Runner(sourcePort,destinationPort,batchSize,threadCount,collections,startTime,endTime);
            runner.transferData();
            System.exit(0);
        } else {
            runner = new Runner(sourcePort, destinationPort, batchSize, threadCount, startTime, endTime, false);
            runner.copy();
            System.exit(0);
        }
    }
}
