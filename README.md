# ms-solr-api-testing
Solr API based Map/Feature services performance testing

Here is the instructions for running the testing locally with SSH tunnel to a DSE cluster in Amazon/Azure Clouds

1. Create SSH tunnel to DSE Cluster

2. Start MAT in the real-time/mat folder with script runmat.sh

3. Clone and build this testing project from https://github.com/FrankXia/ms-solr-api-testing

4. Run FeatureServiceTest from command window such as 

    `java -cp  ./target/ms-solr-api-performance-0.10.15.jar com.esri.arcgis.dse.test.FeatureServiceTester 3`
    
    where the number 3 is the testing case number (range from 0 to 8)