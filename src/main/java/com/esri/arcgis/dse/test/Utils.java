package com.esri.arcgis.dse.test;

public class Utils {

  public static void main(String[] args) {
    if (args.length <= 1) {
      System.out.println("Usage: java -cp ./target/ms-solr-api-performance-1.0.jar com.esri.arcgis.dse.test.Utils <List of numbers> ");
    } else {
      computeStats(args);
    }
  }

  private static void computeStats(String[] data) {
    double sum = 0;
    double min = Double.MAX_VALUE;
    double max = Double.MIN_VALUE;

    double squaredValue = 0.0;
    for (int i=0; i<data.length; i++) {
      long stat = Long.parseLong(data[i]);
      sum += stat;
      if (stat < min) min = stat;
      if (stat > max) max = stat;
      squaredValue += stat * stat;
    }

    double avg = sum / data.length;
    double std_dev = Math.sqrt( (squaredValue - data.length * avg * avg) / (data.length - 1) );

    System.out.println("Average, min, max and std_dev: " + avg +  " " + min + " " +max + " " + std_dev);
  }
}
