package com.esri.arcgis.datastore.test;

public class MapServiceTester {

  public static void main(String[] args) {
    testOneService(args);
    //testAll();
  }

  private static void testOneService(String[] args) {
    if (args == null || args.length < 1) {
      System.out.println("Usage: java -cp ./ms-query-api-performance-1.0-jar-with-dependencies.jar com.esri.arcgis.datastore.test.MapServiceTester <Service Name> {<optional bounding box>}");
      return;
    }
    String serviceName = args[0];
    String host = "localhost";
    int port = 9000;
    int limitMax = 10000;
    int limitMin = 9000;
    String boundingBox = args.length == 2 ? args [1] : GenerateBoundingBox.getBbox(host, port, serviceName, limitMin, limitMax, 180, 90, 1).split("[|]")[0];
    testExportMap(host, port, serviceName, boundingBox);
  }

  private static void testExportMap(String host, int port, String serviceName, String boundingBox) {
    MapService mapService = new MapService(host, port, serviceName, 100);
    mapService.exportMap(boundingBox, 4326);
  }

  private static void testAll() {
    String host = "localhost";
    int port = 9000;

    String[] serviceNames = new String[]{"faa10k", "faa100k", "faa1m", "faa3m", "faa5m", "faa10m", "faa30m", "faa300m"};
    String[] bboxes = new String[serviceNames.length];
    bboxes[0] = "-180,-90,180,90";            // 10000, features returned

    long start = System.currentTimeMillis();
    int limitMax = 10000;
    int limitMin = 9000;
    for (int i=1; i<serviceNames.length; i++) {
      bboxes[i] = GenerateBoundingBox.getBbox(host, port, serviceNames[i], limitMin, limitMax, 180, 90, 1).split("[|]")[0];
    }
    System.out.println("Time to get bounding boxes => " + (System.currentTimeMillis() - start) + " ms");

    for (int index =0; index < serviceNames.length; index++) {
      String name = serviceNames[index];
      MapService mapService = new MapService(host, port, name, 100);
      mapService.exportMap(bboxes[index], 4326);
    }
  }
}
