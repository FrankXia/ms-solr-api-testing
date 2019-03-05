package com.esri.arcgis.datastore.test;

import java.text.DecimalFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class MapServiceAggConcurrentTester {

  public static void main(String[] args) {
    if (args.length >= 6) {
      String hostName = args[0];
      String serviceName = args[1];
      int numThreads = Integer.parseInt(args[2]);
      int numCalls = Integer.parseInt(args[3]);
      int width = Integer.parseInt(args[4]);
      int height = Integer.parseInt(args[5]);
      String aggStyle = "square";
      if (args.length > 6) {
        aggStyle = args[6];
      }
      boolean limitTo3rdQuadrant = true;
      if (args.length > 7) {
        limitTo3rdQuadrant = Boolean.parseBoolean(args[7]);
      }

      concurrentTesting(hostName, serviceName, numThreads, numCalls, width, height, aggStyle, limitTo3rdQuadrant);
    } else {
      System.out.println("Usage: java -cp ./ms-query-api-performance-1.0-jar-with-dependencies.jar com.esri.arcgis.datastore.test.MapServiceAggConcurrentTester " +
          "<Host name> <Service name> <Number of threads> <Number of concurrent calls (<=100)> <Width> <Height> {<Aggregation style> <Limit to 3rd quadrant>}");
    }
  }

  private static Callable<Long> createTask(String host, int port, String serviceName, String boundingBox, String aggStyle) {
    Callable<Long> task = () -> {
      MapService mapService = new MapService(host, port, serviceName);
      return mapService.exportMap(boundingBox, 4326, aggStyle);
    };
    return task;
  }

  private static void concurrentTesting(String host, String serviceName, int numbThreads, int numbConcurrentCalls, int width, int height, String aggStyle, boolean limitTo3rdQuadrant) {
    ExecutorService executor = Executors.newFixedThreadPool(numbThreads);

    int port = 9000;
    DecimalFormat df = new DecimalFormat("#.#");
    List<Callable<Long>> callables = new LinkedList<>();

    try {

      int lineRead = 0;
      while (lineRead < numbConcurrentCalls) {
        String boundingBox = getBbox(width, height, limitTo3rdQuadrant);
        callables.add(createTask(host, port, serviceName, boundingBox, aggStyle));
        lineRead++;
      }

      Stream<Long> results =
          executor.invokeAll(callables)
              .stream()
              .map(future -> {
                try {
                  return future.get();
                } catch (Exception e) {
                  throw new IllegalStateException(e);
                }
              });

      final List<Long> times = new LinkedList<>();
      results.forEach( t -> {
        times.add(t);
      });

      double timeTotal = 0;
      long minTime = times.get(0);
      long maxTime = times.get(0);
      double squaredTimes = 0.0;

      for (int i=0; i<times.size(); i++) {
        timeTotal += times.get(i);
        if (times.get(i) < minTime) minTime = times.get(i);
        if (times.get(i) > maxTime) maxTime = times.get(i);
        squaredTimes += times.get(i) * times.get(i);
      }

      double avgTime = timeTotal / times.size();
      double stdDevTimes = Math.sqrt( (squaredTimes - times.size() * avgTime * avgTime) / (times.size() - 1) );
      System.out.println( "Time -> average, min, max, and standard deviation over " + times.size() +  " requests: | " +  df.format(avgTime) + " | " + df.format(minTime) + " | " + df.format(maxTime)  + " | " + df.format(stdDevTimes) + " | ");


    }catch (Exception ex) {
      ex.printStackTrace();
    }

    try {
      System.out.println("attempt to shutdown executor");
      executor.shutdown();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }
    catch (InterruptedException e) {
      System.err.println("tasks interrupted");
    }
    finally {
      if (!executor.isTerminated()) {
        System.err.println("cancel non-finished tasks");
      }
      executor.shutdownNow();
      System.out.println("shutdown finished");
    }
  }

  static String getBbox(double width, double height, boolean limitTo3rdQuadrant) {
    double minx = 0;
    double maxx = 180;
    double miny = 0;
    double maxy = 90;

    Random random = new Random();
    int signx = random.nextDouble() > 0.5 ? -1: 1;
    int signy = random.nextDouble() > 0.5 ? -1: 1;
    String bbox = null;

    while (true) {
      minx = random.nextDouble() * 180.0;
      if (limitTo3rdQuadrant) {
        minx = minx < 0 ? minx : minx * -1;
      } else {
        minx = minx * signx;
      }
      miny = random.nextDouble() * 90.0;
      if (limitTo3rdQuadrant) {
        miny = miny < 0 ? miny : miny * -1;
      } else {
        miny = miny * signy;
      }

      maxx = minx + width;
      maxy = miny + height;

      bbox = minx + "," + miny + "," + maxx + "," + maxy;

      if (maxx < 180 && maxy < 90) {
        break;
      }
    }

    System.out.println(bbox);
    return bbox;
  }

}
