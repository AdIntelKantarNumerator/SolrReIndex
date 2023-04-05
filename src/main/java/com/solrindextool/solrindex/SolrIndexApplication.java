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

        System.out.println("Enter batch size: ");
        int batchSize = scanner.nextInt();

        System.out.println("Enter thread count: ");
        int threadCount = scanner.nextInt();

        System.out.println("Use custom query: (type y for yes, n for no)");
        scanner.nextLine();
        String custom = scanner.nextLine();
        boolean isCustomQuery;

        if (custom.equalsIgnoreCase("y")){
            isCustomQuery = true;
            System.out.println("Enter your query: ");
            String customQuery = scanner.nextLine();
            SolrIndex solrIndex = new SolrIndex(sourceURL,destinationURL,batchSize,threadCount,isCustomQuery,customQuery);
            solrIndex.copy();
        } else {
            isCustomQuery = false;
            System.out.println("Enter start timestamp: ");
            String startTime = scanner.nextLine();

            System.out.println("Enter end timestamp: ");
            String endTime = scanner.nextLine();

            SolrIndex solrIndex = new SolrIndex(sourceURL,destinationURL,batchSize,threadCount,startTime,endTime,isCustomQuery);
            solrIndex.copy();
        }


    }
}
