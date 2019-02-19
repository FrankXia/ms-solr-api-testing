# ms-solr-api-testing
Solr API based Map/Feature services performance testing

Here is the instructions for running the testing locally with SSH tunnel to a DSE cluster in Amazon/Azure Clouds

1. Build real-time DSE branch, unzip the dist jar file and then remove and add specific jar files, re-zip the whole thing into a single jar file before copying to a cloud server in the DSE Cluster.

2. Create SSH tunnel to DSE Cluster

3. Start MAT/Map/Feature services with a java command

4. Clone and build this testing project from https://github.com/FrankXia/ms-solr-api-testing

5. Run FeatureServiceTest from command window such as 

    `java -cp  ./target/ms-solr-api-performance-0.10.15.jar com.esri.arcgis.dse.test.FeatureServiceTester 3`
    
    where the number 3 is the testing case number (range from 0 to 8)
    
 6. Run concurrent feature service testing from command window such as 

    `java -cp  ./target/ms-solr-api-performance-1.0.jar com.esri.arcgis.dse.test.FeatureServiceConcurrentTester a1 faa30m 3 3 dest  "[ {\"statisticType\":\"avg\",\"onStatisticField\":\"speed\",\"outStatisticFieldName\":\"avg_speed\"}, {\"statisticType\":\"min\",\"onStatisticField\":\"speed\",\"outStatisticFieldName\":\"min_speed\"}, {\"statisticType\":\"max\",\"onStatisticField\":\"speed\",\"outStatisticFieldName\":\"max_speed\"} ]"`
    
 7.  Run concurrent map service testing from command window such as
 
 `java -cp  ./target/ms-solr-api-performance-1.0.jar com.esri.arcgis.dse.test.MapServiceConcurrentTester a1 faa300m 30 30 faa300m.txt 81`
