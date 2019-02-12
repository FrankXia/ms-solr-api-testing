package com.esri.arcgis.dse.test;

import java.util.Random;

public class MapServiceTester {

  public static void main(String[] args) {
    //getBoundingBoxWith10kFeatures(args);
    //testAll();
    testOneService(args);
  }

  private static void testOneService(String[] args) {
    if (args == null || args.length < 1) {
      System.out.println("Usage: java -cp ./target/ms-solr-api-performance-0.10.15.jar com.esri.arcgis.dse.test.MapServiceTester <Service Name> {<optional bounding box>}");
      return;
    }
    String serviceName = args[0];
    String host = "localhost";
    int port = 9000;
    int limit = 10000;
    String boundingBox = args.length == 2 ? args [1] : getBbox(host, port, serviceName, limit);
    testExportMap(host, port, serviceName, boundingBox);
  }

  private static void getBoundingBoxWith10kFeatures(String[] args) {
    if (args == null || args.length < 1) {
      System.out.println("Usage: java -cp ./target/ms-solr-api-performance-0.10.15.jar com.esri.arcgis.dse.test.MapServiceTester <Service Name> ");
      return;
    }

    int limit = 10000;
    String host = "localhost";
    int port = 9000;
    String name = args[0];

    getBbox(host, port, name, limit);
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
      bboxes[i] = getBbox(host, port, serviceNames[i], limit);
    }
    System.out.println("Time to get bounding boxes => " + (System.currentTimeMillis() - start) + " ms");

    for (int index =0; index < serviceNames.length; index++) {
      String name = serviceNames[index];
      MapService mapService = new MapService(host, port, name);
      mapService.exportMap(bboxes[index], 4326);
    }
  }
  private static String getBbox(String host, int port, String serviceName, int limit) {
    double minx = 0;
    double maxx = 180;
    double miny = 0;
    double maxy = 90;

    Random random = new Random();
    minx = random.nextDouble() * 180.0;
    minx = minx < 0 ? minx : minx * -1;
    miny = random.nextDouble() * 90.0;
    miny = miny < 0 ? miny: miny * -1;

    double width = maxx - minx;
    double height = maxy - miny;

    String bbox = minx +"," + miny + "," + maxx + "," + maxy;
    MapService mapService = new MapService(host, port, serviceName);
    long numFeatures = mapService.getCount("1=1", bbox);
    long delta = limit - numFeatures;

    int loopCount = 0;
    while (Math.abs(delta) > 100 || delta < 0) {
      System.out.println(numFeatures + " " + delta);
      double percent  = (double)delta / (double)limit;
      if (Math.abs(percent) > 1) {
        int sign = percent > 0 ? 1: -1;
        width = width + width / 2 * sign;
        height = height + height / 2 * sign;
      } else {
        if (loopCount % 2 == 0)
          width = width + width * percent;
        else
          height = height + height * percent;
      }
      loopCount++;
      maxx = minx + width;
      maxy = miny + height;
      bbox = minx +"," + miny + "," + maxx + "," + maxy;
      numFeatures = mapService.getCount("1=1", bbox);
      delta = limit - numFeatures;
    }
    System.out.println("---------------------------------> " + numFeatures);
    return bbox;
  }

  private static void testExportMap(String host, int port, String serviceName, String boundingBox) {
    MapService mapService = new MapService(host, port, serviceName);
    mapService.exportMap(boundingBox, 4326);
  }
}
