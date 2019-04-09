package com.esri.arcgis.datastore.test;

import java.text.SimpleDateFormat;

public class FeatureServiceWithStatsTester {

  private static SimpleDateFormat simpleDateFormat;

  private static int timeoutInSeconds = 120;

  public static void main(String[] args) {

    int numParameters = args.length;
    if (numParameters >= 4) {
      testVariousRequestsWithStats(args);
    } else {
      testVariousRequestsWithStats(new String[0]);
    }
  }

  private static void testVariousRequestsWithStats(String[] args) {
    if (args.length < 6) {
      System.out.println("Usage: java -cp ./ms-query-api-performance-1.0-jar-with-dependencies.jar com.esri.arcgis.datastore.test.FeatureServiceWithStatsTester <Host name> <Service name> <Group By field name> <Number of runs> <Timeout in seconds> <Out statistics> {<Bounding Box>}");
      System.out.println("Sample:");
      System.out.println("   java -cp  ./ms-query-api-performance-1.0-jar-with-dependencies.jar com.esri.arcgis.datastore.test.FeatureServiceTester localhost faa30m dest 20 120 \"[" +
          "{\\\"statisticType\\\":\\\"avg\\\",\\\"onStatisticField\\\":\\\"speed\\\",\\\"outStatisticFieldName\\\":\\\"avg_speed\\\"}," +
          "{\\\"statisticType\\\":\\\"min\\\",\\\"onStatisticField\\\":\\\"speed\\\",\\\"outStatisticFieldName\\\":\\\"min_speed\\\"}," +
          "{\\\"statisticType\\\":\\\"max\\\",\\\"onStatisticField\\\":\\\"speed\\\",\\\"outStatisticFieldName\\\":\\\"max_speed\\\"}" +
          "]\"");
    } else {
      int serverPort = 9000;
      String host = args[0];
      String serviceName = args[1];
      String groupbyFdName = args[2];
      int numRuns = Integer.parseInt(args[3]);
      if (numRuns <= 0) numRuns = 20;
      timeoutInSeconds = Integer.parseInt(args[4]);
      String outStats = args[5];
      System.out.println(outStats);

      Double[] stats = new Double[numRuns];
      for (int i=0; i<numRuns; i++) {
        String boundingBox = null;
        if (args.length == 7){
          double width = Double.parseDouble(args[6]);
          boundingBox = Utils.getRandomBoundingBox(width, width);
        }
        FeatureService featureService = new FeatureService(host, serverPort, serviceName, timeoutInSeconds);
        Tuple tuple = featureService.doGroupByStats("1=1", groupbyFdName, outStats, boundingBox);
        stats[i] = tuple.requestTime * 1.0;
      }

      Utils.computeStats(stats, numRuns);
    }
  }
}
