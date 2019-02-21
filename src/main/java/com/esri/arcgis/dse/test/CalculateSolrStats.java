package com.esri.arcgis.dse.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class CalculateSolrStats {

  public static void main(String[] args) {
    if (args.length <= 1) {
      System.out.println("Usage: java -cp ./target/ms-solr-api-performance-1.0.jar com.esri.arcgis.dse.test.CalculateSolrStats <File name> <Number of concurrent requests>");
    } else  {
      int numRequests = Integer.parseInt(args[1]);
      computeStats(args[0], numRequests);
    }
  }

  private static void computeStats(String fileName, int numberRequests) {
    File file = new File(fileName);
    String prefix = "Solr request time: ";
    try {
      if (file.exists()) {
        List<Double> data = new LinkedList<>();
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = reader.readLine();
        while (line != null) {
          if (line.startsWith(prefix)) {
            line = line.substring(prefix.length()).trim();
            String[] splits = line.split(" ");
            if (splits.length == 2) {
              data.add((double)Long.parseLong(splits[0]));
              //System.out.println(splits[0]);
            }
          }
          line = reader.readLine();
        }
        reader.close();

        if (numberRequests > data.size()) {
          System.out.println("Error: " + data.size() + " != " + numberRequests);
        } else {
          computeStats(data.toArray(new Double[0]), numberRequests);
        }
      } else {
        System.out.println("File '" + fileName + "' does not exist!");
      }
    }catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  private static void computeStats(Double[] data, int numberRequest) {
    Arrays.sort(data);
    double sum = 0;
    double min = Double.MAX_VALUE;
    double max = Double.MIN_VALUE;
    DecimalFormat df = new DecimalFormat("#.#");

    double squaredValue = 0.0;
    for (int i=(data.length - numberRequest); i < data.length; i++) {
      double stat = data[i];
      sum += stat;
      if (stat < min) min = stat;
      if (stat > max) max = stat;
      squaredValue += stat * stat;
    }

    double avg = sum / numberRequest;
    double std_dev = Math.sqrt( (squaredValue - numberRequest * avg * avg) / (numberRequest - 1) );

    System.out.println("Total data points: " + numberRequest);
    System.out.println("Average, min, max and std_dev: | " + df.format(avg) +  " | " + df.format(min) + " | " + df.format(max) + " | " + df.format(std_dev) + " |");
  }
}