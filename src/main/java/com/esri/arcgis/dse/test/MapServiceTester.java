package com.esri.arcgis.dse.test;

public class MapServiceTester {

  public static void main(String[] args) {
    testOneService(args);
    //testAll();
  }

  private static void testOneService(String[] args) {
    if (args == null || args.length < 1) {
      System.out.println("Usage: java -cp ./target/ms-solr-api-performance-1.0.jar com.esri.arcgis.dse.test.MapServiceTester <Service Name> {<optional bounding box>}");
      return;
    }
    String serviceName = args[0];
    String host = "localhost";
    int port = 9000;
    int limit = 10000;
    String boundingBox = args.length == 2 ? args [1] : GenerateBoundingBox.getBbox(host, port, serviceName, limit, 180, 90,1).split("[|]")[0];
    testExportMap(host, port, serviceName, boundingBox);
  }

  private static void testExportMap(String host, int port, String serviceName, String boundingBox) {
    MapService mapService = new MapService(host, port, serviceName);
    mapService.exportMap(boundingBox, 4326);
  }

  private static void testAll() {
    String host = "localhost";
    int port = 9000;

    String[] serviceNames = new String[]{"faa10k", "faa100k", "faa1m", "faa3m", "faa5m", "faa10m", "faa30m", "faa300m"};
    String[] bboxes = new String[serviceNames.length];
    bboxes[0] = "-180,-90,180,90";            // 10000, features returned

//    bboxes[1] = "-41,-50,30,30";              //  9911
//    bboxes[2] = "30,10,46.8,30.5";            //  9917
//    bboxes[3] = "40.05,10.15,46.59,30.58";    //  9967
//    bboxes[4] = "42.55,10.25,46.45,30.58";    //  9975
//    bboxes[5] = "42.5,21.1,46.5,30.6";        //  9960
//    bboxes[6] = "44.5,25,46.5,30.6";          //  9923

    long start = System.currentTimeMillis();
    int limit = 10000;
    for (int i=1; i<serviceNames.length; i++) {
      bboxes[i] = GenerateBoundingBox.getBbox(host, port, serviceNames[i], limit, 180, 90,1).split("[|]")[0];
    }
    System.out.println("Time to get bounding boxes => " + (System.currentTimeMillis() - start) + " ms");

    for (int index =0; index < serviceNames.length; index++) {
      String name = serviceNames[index];
      MapService mapService = new MapService(host, port, name);
      mapService.exportMap(bboxes[index], 4326);
    }
  }
}
