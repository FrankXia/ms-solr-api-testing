package com.esri.arcgis.dse.test;

import org.json.JSONObject;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Random;
import java.util.TimeZone;

public class FeatureServiceTester {

  private static SimpleDateFormat simpleDateFormat;

  public static void main(String[] args) {

    int numParameters = args.length;
    if (numParameters == 3) {
      testVariousRequestsWithoutStats(args);
    } else if (numParameters >= 4) {
      testVariousRequestsWithStats(args);
    } else {
      testVariousRequestsWithStats(new String[0]);
      System.out.println(" OR ");
      testVariousRequestsWithoutStats(new String[0]);
    }
  }


  private static void testVariousRequestsWithStats(String[] args) {

    if (args.length < 4) {
      System.out.println("Usage: java -cp ./target/ms-solr-api-performance-0.10.15.jar com.esri.arcgis.dse.test.FeatureServiceTester <Host name> <Service name> <Group By field name> <Out statistics> {<Bounding Box>}");
      System.out.println("Sample:");
      System.out.println("   java -cp  ./target/ms-solr-api-performance-0.10.15.jar com.esri.arcgis.dse.test.FeatureServiceTester localhost faa30m dest \"[{\\\"statisticType\\\":\\\"avg\\\",\\\"onStatisticField\\\":\\\"speed\\\",\\\"outStatisticFieldName\\\":\\\"avg_speed\\\"}]\"");
    } else {
      int serverPort = 9000;
      String host = args[0];
      String serviceName = args[1];
      String groupbyFdName = args[2];
      String outStats = args[3];
      System.out.println(outStats);

      String boundingBox = (args.length == 5)? args[4] : null;

      FeatureService featureService = new FeatureService(host, serverPort, serviceName);
      featureService.doGroupByStats("1=1", groupbyFdName, outStats, boundingBox);

    }
  }


  private static void testVariousRequestsWithoutStats(String[] args) {

    if (args.length < 3) {
      System.out.println("Usage: java -cp ./target/ms-solr-api-performance-0.10.15.jar com.esri.arcgis.dse.test.FeatureServiceTester <Host name> <Service name> <Option codes: 0 -> 8> ");
      System.out.println("Code stands for: ");
      System.out.println("0 -> get total counts for all services ");
      System.out.println("1 -> all:  1=1, limit=10,000 ");
      System.out.println("2 -> attribute range:  speed < x and speed > y, limit=10,000 for all services ");
      System.out.println("3 -> attribute group:  flightId IN ('1234', '5678'), limit=10,000 for all services ");
      System.out.println("4 -> spatial extent:  geometry INSIDE bounding box, limit=10,000 for all services ");
      System.out.println("5 -> not supported yet, spatial polygon:  geometry INSIDE state boundary, limit=10,000 for all services ");
      System.out.println("6 -> temporal extent:  time > t1 and time < t2, limit=10,000 for all services ");
      System.out.println("7 -> spatiotemporal extent:  geometry INSIDE bounding box AND time > t1 and time < t2, limit=10,000 for all services ");
      System.out.println("8 -> spatiotemporal extent with attribute group:  flightId IN ('1234', '5678') AND geometry INSIDE bounding box AND time > t1 and time < t2, limit=10,000 for all services ");

      System.out.println("Samples: ");
      System.out.println("java -cp  ./target/ms-solr-api-performance-0.10.15.jar com.esri.arcgis.dse.test.FeatureServiceTester localhost faa10m 1,2,3");
    } else {

      int serverPort = 9000;
      String[] tableNames = new String[]{args[1]}; // new String[]{"faa10k", "faa100k", "faa1m", "faa3m", "faa5m", "faa10m", "faa30m", "faa300m"};

      String pattern = "yyyy-MM-dd HH:mm:ss";
      simpleDateFormat = new SimpleDateFormat(pattern);
      simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

      String hostName = args[0];
      String codes = args[2];
      if (codes.contains("0")) testTotalCountForAll(hostName, serverPort, tableNames);
      if (codes.contains("1")) testGetFeaturesForAll(hostName, serverPort, tableNames);
      if (codes.contains("2")) testGetFeaturesWithSpeedRange(hostName, serverPort, tableNames);
      if (codes.contains("3")) testGetFeaturesWithSQLIn(hostName, serverPort, tableNames);
      if (codes.contains("4")) testGetFeaturesWithBoundingBox(hostName, serverPort, tableNames, 20);
      if (codes.contains("5")) System.out.println("To be implemented!");

      if (codes.contains("6")) testGetFeaturesWithTimeExtent(hostName, serverPort, tableNames);
      if (codes.contains("7")) testGFeaturesWithBoundingBoxAndTimeExtent(hostName, serverPort, tableNames, 35);
      // if bounding box too small, the table/service may return 0 feature.
      if (codes.contains("8")) testGFeaturesWithBoundingBoxAndTimeExtentAndSQLIN(hostName, serverPort, tableNames, 60);
    }
  }

  private static void testGFeaturesWithBoundingBoxAndTimeExtentAndSQLIN(String hostName, int port, String[] tableNames, double boundingBoxWidth) {
    System.out.println("======== get features from each service with a 10 degree random bounding box and time extent and IN parameter ========= ");

    String fieldName = "orig";
    boolean isStringField = true;

    String boundingBox = generateBoundingBox(boundingBoxWidth);
    String timeFieldName = "ts";

    try {
      for (String table : tableNames) {
        FeatureService featureService = new FeatureService(hostName, port, table);
        String mTimestamp = getTimeExtent(featureService, timeFieldName);
        String where = getUniqueValuesForIN(featureService, fieldName, isStringField);
        featureService.getFeaturesWithWhereClauseAndBoundingBoxAndTimeExtent(where, boundingBox, mTimestamp);
      }
    }catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  private static void testGFeaturesWithBoundingBoxAndTimeExtent(String hostName, int port, String[] tableNames, double boundingBoxWidth) {
    System.out.println("======== get features from each service with a 10 degree random bounding box and time extent ========= ");
    String boundingBox = generateBoundingBox(boundingBoxWidth);

    String fieldName = "ts";
    try {
      for (String table : tableNames) {
        FeatureService featureService = new FeatureService(hostName, port, table);
        String mTimestamp = getTimeExtent(featureService, fieldName);
        featureService.getFeaturesWithWhereClauseAndBoundingBoxAndTimeExtent("1=1", boundingBox, mTimestamp);
      }
    }catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  private static void testGetFeaturesWithSQLIn(String hostName, int port, String[] tableNames) {
    System.out.println("======== get features from each service with sSQL IN (xxx,xxx) ========= ");
    String fieldName = "plane_id";
    boolean isStringField = false;

    try {
      for (String table : tableNames) {
        FeatureService featureService = new FeatureService(hostName, port, table);
        String where = getUniqueValuesForIN(featureService, fieldName, isStringField);
        featureService.getFeaturesWithWhereClause(where);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  private static void testGetFeaturesWithTimeExtent(String hostName, int port, String[] tableNames) {
    System.out.println("======== get features from each service with time filter ========= ");
    String fieldName = "ts";
    String pattern = "yyyy-MM-dd HH:mm:ss";
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
    simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    try {
      for (String table : tableNames) {
        FeatureService featureService = new FeatureService(hostName, port, table);
        String mTimestamp = getTimeExtent(featureService, fieldName);
        featureService.getFeaturesWithTimeExtent("1=1", mTimestamp);
      }
    }catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  private static void testGetFeaturesWithSpeedRange(String hostName, int port, String[] tableNames) {
    System.out.println("======== get features from each service with speed range ========= ");
    String fieldName = "speed";
    testWithStatsAsWhereClause(fieldName, hostName, port, tableNames);
  }

  private static void testGetFeaturesWithBoundingBox(String hostName, int port, String[] tableNames, double boundingBoxWidth) {
    System.out.println("======== get features from each service with a 10 degree random bounding box ========= ");
    String boundingBox = generateBoundingBox(boundingBoxWidth);
    for (String table: tableNames) {
      FeatureService featureService = new FeatureService(hostName, port, table);
      featureService.getFeaturesWithWhereClauseAndBoundingBox("1=1", boundingBox);
    }
  }
  
  private static void testGetFeaturesForAll(String hostName, int port, String[] tableNames) {
    System.out.println("======== get features from each service with a random offset ========= ");
    for (String table: tableNames) {
      FeatureService featureService = new FeatureService(hostName, port, table);
      boolean useOffset =  !table.contains("10k");
      featureService.getFeaturesWithWhereClauseAndRandomOffset("1=1", useOffset);
    }
  }
  
  private static void testTotalCountForAll(String hostName, int port, String[] tableNames) {
    System.out.println("======== get total count for each service ========= ");
    for (String table: tableNames) {
      FeatureService featureService = new FeatureService(hostName, port, table);
      featureService.getCount("1=1");
    }
  }

  private static void testWithStatsAsWhereClause(String fieldName, String hostName, int port, String[] tableNames) {
    for (String table: tableNames) {
      FeatureService featureService = new FeatureService(hostName, port, table);
      JSONObject stats = featureService.getStates(fieldName);
      double min = stats.getDouble("min");
      double max = stats.getDouble("max");
//      double random = new Random().nextDouble() * (max - min);
//      random = random < 0 ? random * (-1) : random;
      double random = 0.5 * (max - min);
      String where = fieldName + " > " + min + " AND " + fieldName + " < " + (min + random);
      featureService.getFeaturesWithWhereClause(where);
    }
  }

  private static String generateBoundingBox(double width) {
    Random random = new Random();
    boolean sign = random.nextBoolean();

    double lon = random.nextDouble();
    double lat = random.nextDouble();

    lon = sign? lon * -180 : lon * 180;
    lat = sign? lat * -90 : lat * 90;

    double minx = lon;
    double maxx = lon;
    if (lon + width/2.0 > 180.0) {
      maxx = 180;
      minx = maxx - width;
    } else if (lon - width/2.0 < -180) {
      minx = -180;
      maxx = minx + width;
    } else {
      minx = lon - width/2.0;
      maxx = lon + width/2.0;
    }

    double miny = lat;
    double maxy = lat;
    if (lat + width/2.0 > 90.0) {
      maxy = 90;
      miny = maxy - width;
    } else if (lat - width/2.0 < -90) {
      miny = -90;
      maxy = minx + width;
    } else {
      miny = lat - width/2.0;
      maxy = lat + width/2.0;
    }

    return minx + "," + miny + "," + maxx + "," + maxy;
  }

  private static String getTimeExtent(FeatureService featureService, String fieldName) throws Exception {
    JSONObject stats = featureService.getStates(fieldName);
    String minTimestamp = stats.getString("min").replace("T", " ").replace("Z", "");
    String maxTimestamp = stats.getString("max").replace("T", " ").replace("Z", "");
    long min = simpleDateFormat.parse(minTimestamp).getTime();
    long max = simpleDateFormat.parse(maxTimestamp).getTime();
//    double random = new Random().nextDouble() * (max - min);
//    long randomLong = (long) (random < 0 ? random * (-1) : random);
    long randomLong = (long) (0.5 * (max - min));
    String mTimestamp = min + ","+ (min + randomLong);
    return mTimestamp;
  }

  private static String getUniqueValuesForIN(FeatureService featureService, String fieldName, boolean isStringField) throws Exception {
    Random random = new Random();

    List<String> uniqueValues = featureService.getUniqueValues(fieldName);
    if (uniqueValues.size() == 0) throw new Exception("No unique values found!");
    if (uniqueValues.size() == 1) throw new Exception("Only have one value.");
    String uniqueValue1 = uniqueValues.get(0);
    String uniqueValue2 = uniqueValues.get(1);
    int totalCount = uniqueValues.size();
    if (totalCount > 2) {
      int index = random.nextInt() % totalCount;
      index = index < 0 ? (-1) * index : index;
      uniqueValue1 = uniqueValues.get(index);

      index = random.nextInt() % totalCount;
      index = index < 0 ? (-1) * index : index;
      while (uniqueValues.get(index).equals(uniqueValue1)) {
        index = random.nextInt() % totalCount;
        index = index < 0 ? (-1) * index : index;
      }
      uniqueValue2 = uniqueValues.get(index);
    }

    if (isStringField) {
      uniqueValue1 = "'" + uniqueValue1.replaceAll("'", "''") + "'";
      uniqueValue2 = "'" + uniqueValue2.replaceAll("'", "''") + "'";
    }

    String where = fieldName + " IN (" + uniqueValue1 + "," + uniqueValue2 + ")";
    return where;
  }
}
