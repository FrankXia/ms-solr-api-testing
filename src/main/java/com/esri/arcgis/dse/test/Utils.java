package com.esri.arcgis.dse.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.LinkedList;
import java.util.List;

public class Utils {

  public static void main(String[] args) {
    if (args.length == 0) {
      System.out.println("Usage: java -cp ./target/ms-solr-api-performance-1.0.jar com.esri.arcgis.dse.test.Utils <List of numbers> Or <File name>");
    } else if (args.length == 1) {
      computeStats(args[0]);
    } else {
      computeStats(args);
    }
  }

  private static void computeStats(String fileName) {
    File file = new File(fileName);
    String prefix = "Solr request time: ";
    try {
      if (file.exists()) {
        List<String> data = new LinkedList<>();
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = reader.readLine();
        while (line != null) {
          if (line.startsWith(prefix)) {
            line = line.substring(prefix.length()).trim();
            String[] splits = line.split(" ");
            if (splits.length == 2) {
              data.add(splits[0]);
              System.out.println(splits[0]);
            }
          }
          line = reader.readLine();
        }
        reader.close();

        computeStats(data.toArray(new String[0]));
      } else {
        System.out.println("File '" + fileName + "' does not exist!");
      }
    }catch (Exception ex) {
      ex.printStackTrace();
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

    System.out.println("Total data points: " + data.length);
    System.out.println("Average, min, max and std_dev: " + avg +  " " + min + " " +max + " " + std_dev);
  }
}
