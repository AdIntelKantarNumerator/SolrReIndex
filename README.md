# Solr Reindex Tool

java application which will take the following customisable parameters and based on these parameters query source solr and copy that data to destination solr:

* Time range (use timestamp field of solr)

* Source ip port

* Destination ip port

* Batch size

* Parallel thread count


### To Run

For further reference, please consider the following sections:

1. Start the Application with SolrIndexApplication main class
2. Enter the fields asked in console
3. Format for Source and Destination port should be like (it should include collection name): "http://10.100.50.158:38209/solr/brand-n_v6_1_occurrence/"
4. Format for Custom query should be like: "http://10.100.50.158:38209/solr/brand-n_v6_1_occurrence/select?fq=first_run_date:([2021-01-01T05:00:00.000Z TO 2021-02-01T03:59:59.999Z])&q=*:*"
5. Batch size should not be more than 5000 to avoid stackoverflow error
6. Data transfer process will be started after enter all fields



