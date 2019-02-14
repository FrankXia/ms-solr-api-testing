package com.esri.arcgis.dse.test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Random;

public class GenerateBoundingBox {

  public static void main(String[] args) {
    getBoundingBoxWith10kFeatures(args);
  }

  private static void getBoundingBoxWith10kFeatures(String[] args) {
    if (args == null || args.length < 2) {
      System.out.println("Usage: java -cp ./target/ms-solr-api-performance-1.0.jar com.esri.arcgis.dse.test.GenerateBoundingBox <Service Name> <Output File>");
      return;
    }

    int limit = 10000;
    String host = "localhost";
    int port = 9000;
    String name = args[0];

    String fileName = "./" + args[1];

    try {
      BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));

      int validCount = 0;
      while (validCount < 100) {
        String boundingBox = getBbox(host, port, name, limit, validCount);
        writer.write(boundingBox);
        writer. newLine();
        validCount++;
      }

      writer.close();
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  static String getBbox(String host, int port, String serviceName, int limit, int count) {
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

      maxx = 180;
      maxy = 90;

      double width = maxx - minx;
      double height = maxy - miny;

      bbox = minx + "," + miny + "," + maxx + "," + maxy;
      MapService mapService = new MapService(host, port, serviceName);
      numFeatures = mapService.getCount("1=1", bbox);
      bbox = bbox + "|" + numFeatures;

      long delta = limit - numFeatures;
      int loopCount = 0;

      while (Math.abs(delta) > 100 || delta < 0) {
        System.out.println(numFeatures + " " + delta + " " + loopCount);
        double percent = (double) delta / (double) limit;
        if (Math.abs(percent) > 1) {
          int sign = percent > 0 ? 1 : -1;
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