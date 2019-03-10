package com.esri.arcgis.datastore.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.DecimalFormat;
import java.util.*;

public class CalculateStats {

  public static void main(String[] args) {

    if (args.length < 3) {
      System.out.println("Usage: java -cp ./ms-query-api-performance-1.0-jar-with-dependencies.jar com.esri.arcgis.datastore.test.CalculateStats <File name> <Number of concurrent requests> <Prefix> {<Datastore request multiples>}");
      System.out.println("Sample: ");
      System.out.println("  java -cp ./ms-query-api-performance-1.0-jar-with-dependencies.jar com.esri.arcgis.datastore.test.CalculateStats z100 100 \"Solr request time: \"");
      System.out.println("  java -cp ./ms-query-api-performance-1.0-jar-with-dependencies.jar com.esri.arcgis.datastore.test.CalculateStats z100 100 \"Elastic query time: \" 2");
    } else  {
      String fileName = args[0];
      int numRequests = Integer.parseInt(args[1]);
      String prefix =  args[2]; //  "Solr request time: ";
      int datastoreRequestRatio = 1;

      if (args.length > 4) {
        datastoreRequestRatio = Integer.parseInt(args[4]);
      }


      // computeStatsForSimpleRequestTimeOnly(fileName, numRequests, prefix);
      computeStats(fileName, numRequests, prefix, datastoreRequestRatio);
    }
  }

  private static void computeStatsForSimpleRequestTimeOnly(String fileName, int numberRequests, String prefix) {
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
          computeStatsForSingpleRequestTimeOnly(valueArray, numberRequests);

          if (featuresList.size() >= numberRequests) {
            computeStatsForSingpleRequestTimeOnly(featuresList.toArray(new Double[0]), numberRequests);
          }
        }
      } else {
        System.out.println("File '" + fileName + "' does not exist!");
      }
    }catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  private static void computeStatsForSingpleRequestTimeOnly(Double[] data, int numberRequest) {
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

  private static void computeStats(String fileName, int numberRequests, String prefix, int datastoreRequestRatio) {
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

        if (numberRequests * datastoreRequestRatio > timeFeatures.size()) {
          System.out.println("Error: " + timeFeatures.size() + " < " + datastoreRequestRatio * numberRequests);
          Collections.sort(timeFeatures, new SortByFeatures());
          for (int i=0; i < timeFeatures.size(); i++) {
            System.out.println(timeFeatures.get(i).features + " : " + timeFeatures.get(i).time);
          }
        } else {
          int extra = timeFeatures.size() - numberRequests * datastoreRequestRatio;
          if (extra > 0) {
            System.out.println("WARNING: the number of data store requests is greater than concurrent requests of "  + numberRequests + ", " + extra/2);
          }

          Double[] times = new Double[numberRequests];
          Double[] features = new Double[numberRequests];
          int index = 0;
          Collections.sort(timeFeatures, new SortByFeatures());
          for (int i=extra; i < timeFeatures.size(); i = i + datastoreRequestRatio) {
            TimeFeature timeFeature1 = timeFeatures.get(i);
            if (datastoreRequestRatio == 1) {
              times[index] = timeFeature1.time;
              features[index] = (double) (timeFeature1.features);
            } else if (datastoreRequestRatio == 2) {
              TimeFeature timeFeature2 = timeFeatures.get(i + 1);
              times[index] = timeFeature1.time + timeFeature2.time;
              features[index] = (double) ((timeFeature1.features + timeFeature2.features) / datastoreRequestRatio);
            }
            index++;
          }

          Utils.computeStats(times, numberRequests);
          Utils.computeStats(features, numberRequests);
        }
      } else {
        System.out.println("File '" + fileName + "' does not exist!");
      }
    }catch (Exception ex) {
      ex.printStackTrace();
    }
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