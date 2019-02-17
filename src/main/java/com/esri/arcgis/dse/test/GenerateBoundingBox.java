package com.esri.arcgis.dse.test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Random;

public class GenerateBoundingBox {

  public static void main(String[] args) {
    getBoundingBoxWith10kFeatures(args);
  }

  private static void getBoundingBoxWith10kFeatures(String[] args) {
    if (args == null || args.length < 3) {
      System.out.println("Usage: java -cp ./target/ms-solr-api-performance-1.0.jar com.esri.arcgis.dse.test.GenerateBoundingBox " +
          "<Host Name> <Service Name> <Output File> { <# of bounding boxes: 100> <width: 180> <height: 90>}");
      return;
    }

    int port = 9000;
    int limit = 10000;

    String host = args[0];
    String name = args[1];
    String fileName = "./" + args[2];

    int numBBoxes = 100;
    double width = 180;
    double height = 90;
    if (args.length > 3) numBBoxes = Integer.parseInt(args[3]);
    if (args.length > 4) width = Double.parseDouble(args[4]);
    if (args.length > 5) height = Double.parseDouble(args[5]);

    try {
      BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));

      int validCount = 0;
      while (validCount < numBBoxes) {
        String boundingBox = getBbox(host, port, name, limit, width, height, validCount);
        writer.write(boundingBox);
        writer. newLine();
        validCount++;
      }

      writer.close();
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  static String getBbox(String host, int port, String serviceName, int limit, double initWidth, double initHeight, int count) {
    double minx = 0;
    double maxx = 180;
    double miny = 0;
    double maxy = 90;

    Random random = new Random();
    String bbox = null;
    long numFeatures = 0;
    while (true) {
      minx = random.nextDouble() * 180.0;
      minx = minx < 0 ? minx : minx * -1;
      miny = random.nextDouble() * 90.0;
      miny = miny < 0 ? miny : miny * -1;

      maxx = initWidth;
      maxy = initHeight;

      double width = maxx - minx;
      double height = maxy - miny;

      bbox = minx + "," + miny + "," + maxx + "," + maxy;
      MapService mapService = new MapService(host, port, serviceName);
      numFeatures = mapService.getCount("1=1", bbox);
      bbox = bbox + "|" + numFeatures;

      long delta = limit - numFeatures;
      int loopCount = 0;

      while (Math.abs(delta) > 100 || delta < 0) {
        double percent = (double) delta / (double) limit;
        System.out.println("# of features: " + numFeatures + ", delta: " + delta + ", loop count: " + loopCount + ", percentage: " + percent);
        if (Math.abs(percent) >= 1) {
          percent = percent > 0 ? 1/2.0 : -1/2.0;
          width = width + width * percent;
          height = height + height * percent;
        } else {
          if (loopCount % 2 == 0)
            width = width + width * percent;
          else
            height = height + height * percent;
        }
        loopCount++;
        maxx = (minx + width) % 180.0;
        maxy = (miny + height) % 90.0;
        bbox = minx + "," + miny + "," + maxx + "," + maxy;
        numFeatures = mapService.getCount("1=1", bbox);
        delta = limit - numFeatures;
        bbox = bbox  + "|" + numFeatures;

        if (loopCount > 50) {
          bbox = null;
          break;
        }
      }
      if (bbox != null) break;
    }
    System.out.println( count + " ---------------------------------> " + numFeatures + " " + bbox);
    return bbox;
  }

}
