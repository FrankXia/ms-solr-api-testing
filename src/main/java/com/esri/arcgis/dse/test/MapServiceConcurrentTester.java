package com.esri.arcgis.dse.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class MapServiceConcurrentTester {

  public static void main(String[] args) {
    if (args.length == 4) {
      String serviceName = args[0];
      int numThreads = Integer.parseInt(args[1]);
      int numCalls = Integer.parseInt(args[2]);
      String fileName = args[3];
      concurrentTesting(serviceName, numThreads, numCalls, fileName);
    } else {
      System.out.println("Usage: java -cp ./target/ms-solr-api-performance-1.0.jar com.esri.arcgis.dse.test.MapServiceConcurrentTester <Service name> <Number of threads> <Number of concurrent calls (<=100)> <Path to bounding box file>");
    }
  }

  private static Callable<Long> createTask(String host, int port, String serviceName, String boundingBox) {
    Callable<Long> task = () -> {
      MapService mapService = new MapService(host, port, serviceName);
      return mapService.exportMap(boundingBox, 4326);
    };
    return task;
  }

  private static void concurrentTesting(String serviceName, int numbThreads, int numbConcurrentCalls, String bboxFile) {
    ExecutorService executor = Executors.newFixedThreadPool(numbThreads);

    String host = "localhost";
    int port = 9000;

    List<Callable<Long>> callables = new LinkedList<>();

    try {
      BufferedReader reader = new BufferedReader(new FileReader(bboxFile));
      String line = reader.readLine();
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
      long totalFinal = results.reduce(0L, (total, i) -> total + i);
      System.out.println( (double)totalFinal / (double)callables.size());

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