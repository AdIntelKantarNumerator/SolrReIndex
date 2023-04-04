package com.solrindextool.solrindex;

import com.solrindextool.solrindex.service.SolrIndex;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Scanner;

@SpringBootApplication
public class SolrIndexApplication {

    public static void main(String[] args){
        SpringApplication.run(SolrIndexApplication.class, args);
        Scanner scanner = new Scanner(System.in);

        System.out.println("Enter source Port: ");
        String sourceURL = scanner.nextLine();

        System.out.println("Enter Destination Port: ");
        String destinationURL = scanner.nextLine();

        System.out.println("Enter start timestamp: ");
        String startTime = scanner.nextLine();

        System.out.println("Enter end timestamp: ");
        String endTime = scanner.nextLine();

        System.out.println("Enter batch size: ");
        int batchSize = scanner.nextInt();

        System.out.println("Enter thread count: ");
        int threadCount = scanner.nextInt();

        SolrIndex solrIndex = new SolrIndex(sourceURL,destinationURL,batchSize,threadCount,startTime,endTime);
        solrIndex.copy();
    }
}
