package com.esri.arcgis.dse.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.DecimalFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class MapServiceConcurrentTester {

  public static void main(String[] args) {
    if (args.length == 6) {
      String hostName = args[0];
      String serviceName = args[1];
      int numThreads = Integer.parseInt(args[2]);
      int numCalls = Integer.parseInt(args[3]);
      String fileName = args[4];
      int lines2Skip = Integer.parseInt(args[5]);
      concurrentTesting(hostName, serviceName, numThreads, numCalls, fileName, lines2Skip);
    } else {
      System.out.println("Usage: java -cp ./ms-solr-api-performance-1.0.jar com.esri.arcgis.dse.test.MapServiceConcurrentTester " +
          "<Host name> <Service name> <Number of threads> <Number of concurrent calls (<=100)> <Path to bounding box file> <Number of lines to skip>");
    }
  }

  private static Callable<Long> createTask(String host, int port, String serviceName, String boundingBox) {
    Callable<Long> task = () -> {
      MapService mapService = new MapService(host, port, serviceName);
      return mapService.exportMap(boundingBox, 4326);
    };
    return task;
  }

  private static void concurrentTesting(String host, String serviceName, int numbThreads, int numbConcurrentCalls, String bboxFile, int lines2Skip) {
    ExecutorService executor = Executors.newFixedThreadPool(numbThreads);

    int port = 9000;
    DecimalFormat df = new DecimalFormat("#.#");
    List<Callable<Long>> callables = new LinkedList<>();

    try {
      BufferedReader reader = new BufferedReader(new FileReader(bboxFile));
      String line = reader.readLine();

      while (line != null && lines2Skip > 0) {
        line = reader.readLine();
        lines2Skip--;
      }

      int lineRead = 0;
      while (line != null && lineRead < numbConcurrentCalls) {
        String boundingBox = line.split("[|]")[0];
        callables.add(createTask(host, port, serviceName, boundingBox));
        lineRead++;
        line = reader.readLine();
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
}
