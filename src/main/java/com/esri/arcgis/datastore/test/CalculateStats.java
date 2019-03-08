package com.esri.arcgis.datastore.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.DecimalFormat;
import java.util.*;

public class CalculateStats {

  public static void main(String[] args) {
    if (args.length < 3) {
      System.out.println("Usage: java -cp ./ms-query-api-performance-1.0-jar-with-dependencies.jar com.esri.arcgis.datastore.test.CalculateStats <File name> <Number of concurrent requests> <Prefix>");
      System.out.println("Sample: ");
      System.out.println("  java -cp ./ms-query-api-performance-1.0-jar-with-dependencies.jar com.esri.arcgis.datastore.test.CalculateStats z100 100 \"Solr request time: \"");
      System.out.println("  java -cp ./ms-query-api-performance-1.0-jar-with-dependencies.jar com.esri.arcgis.datastore.test.CalculateStats z100 100 \"Elastic query time: \"");
    } else  {
      String fileName = args[0];
      int numRequests = Integer.parseInt(args[1]);
      String prefix =  args[2]; //  "Solr request time: ";

      //computeStatsForAggregationESTimeOnly(fileName, numRequests, prefix);
      computeStats(fileName, numRequests, prefix);
    }
  }

 private  static DecimalFormat df = new DecimalFormat("#.#");

  private static void computeStatsForAggregationESTimeOnly(String fileName, int numberRequests, String prefix) {
    File file = new File(fileName);
    try {
      if (file.exists()) {
        String secondSeparateString = "total:";
        List<Double> data = new LinkedList<>();
        List<Double> featuresList = new LinkedList<>();
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = reader.readLine();
        while (line != null) {
          if (line.startsWith(prefix)) {
            line = line.substring(prefix.length()).trim();
            String[] splits = line.split(" ");
            if (splits.length >= 2) {
              data.add((double)Long.parseLong(splits[0]));
              int index = line.indexOf(secondSeparateString);
              //System.out.println(line + " " + index);
              if (index > 0) {
                line = line.substring(index + secondSeparateString.length());
                index = line.indexOf(", ");
                if (index > 0) {
                  int featureCount = Integer.parseInt(line.substring(0, index));
                  featuresList.add(featureCount*1.0);
                }
              }
            }
          }
          line = reader.readLine();
        }
        reader.close();

        if (numberRequests > data.size()) {
          System.out.println("Error: " + data.size() + " != " + numberRequests);
        } else {
          Double[] valueArray = data.toArray(new Double[0]);
          Arrays.sort(valueArray);
          computeStatsForAggregationESTimeOnly(valueArray, numberRequests);

          if (featuresList.size() >= numberRequests) {
            computeStatsForAggregationESTimeOnly(featuresList.toArray(new Double[0]), numberRequests);
          }
        }
      } else {
        System.out.println("File '" + fileName + "' does not exist!");
      }
    }catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  private static void computeStatsForAggregationESTimeOnly(Double[] data, int numberRequest) {
    Arrays.sort(data);
    double sum = 0;
    double min = Double.MAX_VALUE;
    double max = Double.MIN_VALUE;
    DecimalFormat df = new DecimalFormat("#.#");

    double squaredValue = 0.0;
    for (int i=(data.length - 1); i >= (data.length - numberRequest); i--) {
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

  private static void computeStats(String fileName, int numberRequests, String prefix) {
    File file = new File(fileName);
    try {
      if (file.exists()) {
        String secondSeparateString = "total:";

        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = reader.readLine();

        List<TimeFeature> timeFeatures = new LinkedList<>();
        int count = 0;
        while (line != null) {
          if (line.trim().startsWith(prefix)) {
            count++;
            line = line.substring(prefix.length());
            double time = Double.parseDouble(line.split(" ")[0]);
            line = line.substring(line.indexOf(secondSeparateString)+6);
            long features = Long.parseLong(line.split(",")[0]);
            System.out.println(time + " " + features +"     " + line);

            TimeFeature timeFeature = new TimeFeature(time, features);
            timeFeatures.add(timeFeature);
          }

          line = reader.readLine();
        }

        reader.close();
        System.out.println(count + ", # of requests: " + timeFeatures.size());

        if (numberRequests * 2 != timeFeatures.size()) {
          System.out.println("Error: " + timeFeatures.size() + " != " + numberRequests);
        } else {

          Double[] times = new Double[numberRequests];
          Double[] features = new Double[numberRequests];
          int index = 0;
          Collections.sort(timeFeatures, new SortByFeatures());
          for (int i=0; i<timeFeatures.size(); i = i + 2) {
            TimeFeature timeFeature1 = timeFeatures.get(i);
            TimeFeature timeFeature2 = timeFeatures.get(i+1);
            times[index] = timeFeature1.time + timeFeature2.time;
            features[index] = (double) ((timeFeature1.features + timeFeature2.features)/2);
            index++;
          }

          Arrays.sort(times);
          computeStats(times, numberRequests);
          computeStats(features, numberRequests);
        }
      } else {
        System.out.println("File '" + fileName + "' does not exist!");
      }
    }catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  static void computeStats(Double[] data, int numberRequest) {
    Arrays.sort(data);
    double sum = 0;
    double min = Double.MAX_VALUE;
    double max = Double.MIN_VALUE;

    double squaredValue = 0.0;
    for (int i= 0; i < numberRequest; i++) {
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

class TimeFeature {
  double time;
  long features;

  public TimeFeature(double time, long features) {
    this.time = time;
    this.features = features;
  }
}

class SortByFeatures implements Comparator<TimeFeature> {

  public int compare(TimeFeature timeFeature1, TimeFeature timeFeature2) {
    return (int)(timeFeature1.features - timeFeature2.features);
  }
}