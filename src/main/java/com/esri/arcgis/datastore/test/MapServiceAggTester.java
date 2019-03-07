package com.esri.arcgis.datastore.test;

import java.util.Random;

public class MapServiceAggTester {
  public static void main(String[] args) {
    if (args.length >= 5) {
      String hostName = args[0];
      String serviceName = args[1];
      int numCalls = Integer.parseInt(args[2]);
      int width = Integer.parseInt(args[3]);
      int height = Integer.parseInt(args[4]);
      String aggStyle = "square";
      if (args.length > 5)
        aggStyle = args[5];

      int timeoutInSeconds = 60;
      if (args.length > 6) {
        timeoutInSeconds = Integer.parseInt(args[6]);
      }

      singleTesting(hostName, serviceName, width, height, aggStyle, numCalls, timeoutInSeconds);

    } else {
      System.out.println("Usage: java -cp ./ms-query-api-performance-1.0-jar-with-dependencies.jar com.esri.arcgis.datastore.test.MapServiceAggTester " +
          "<Host name> <Service name> <Number of calls> <Bounding box width> <Bounding box height> {<Aggregation style> <Timeout in seconds>}");
    }
  }

  private static void singleTesting(String host, String serviceName, int width, int height, String aggStyle, int numCalls, int timeoutInSeconds) {
    int port = 9000;
    MapService mapService = new MapService(host, port, serviceName);
    Double[] times = new Double[numCalls];
    for (int index=0; index < numCalls; index++) {
      String boundingBox = getBbox(width, height);
      long time = mapService.exportMap(boundingBox, 4326, aggStyle, timeoutInSeconds);
      times[index] = time * 1.0;
    }
    CalculateStats.computeStats(times, numCalls);
  }

  private static Random random = new Random();
  static String getBbox(double width, double height) {
    double MAX_W = 360;
    double MAX_H = 180;
    double MIN_X = -180;
    double MIN_Y = -90;

    double randomX = random.nextDouble();
    double randomY = random.nextDouble();
    double minx = MIN_X + randomX * (MAX_W - width);
    double miny = MIN_Y + randomY * (MAX_H - height);
    String bbox = minx +"," + miny + "," + (minx + width) + "," + (miny + height);
    System.out.println(bbox);
    return bbox;
  }
}
