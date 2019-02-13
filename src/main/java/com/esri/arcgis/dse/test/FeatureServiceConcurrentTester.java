package com.esri.arcgis.dse.test;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class FeatureServiceConcurrentTester {

  private static Random random = new Random();

  public static void main(String[] args) {
    if (args.length == 5) {
      String serviceName = args[0];
      int numThreads = Integer.parseInt(args[1]);
      int numCalls = Integer.parseInt(args[2]);
      String groupByFieldName = args[3];
      String outStatisitcs = args[4];
      concurrentTesting(serviceName, numThreads, numCalls, groupByFieldName, outStatisitcs);
    } else {
      System.out.println("Usage: java -cp ./target/ms-solr-api-performance-1.0.jar com.esri.arcgis.dse.test.FeatureServiceConcurrentTester <Service name> <Number of threads> <Number of concurrent calls (<=100)> <Group By field name> <Out Statistics>");
      System.out.println("Sample:");
      System.out.println("   java -cp  ./target/ms-solr-api-performance-1.0.jar com.esri.arcgis.dse.test.FeatureServiceConcurrentTester faa30m 4 8 dest  \"[" +
          " {\\\"statisticType\\\":\\\"avg\\\",\\\"onStatisticField\\\":\\\"speed\\\",\\\"outStatisticFieldName\\\":\\\"avg_speed\\\"}," +
          " {\\\"statisticType\\\":\\\"min\\\",\\\"onStatisticField\\\":\\\"speed\\\",\\\"outStatisticFieldName\\\":\\\"min_speed\\\"}," +
          " {\\\"statisticType\\\":\\\"max\\\",\\\"onStatisticField\\\":\\\"speed\\\",\\\"outStatisticFieldName\\\":\\\"max_speed\\\"} " +
          "]\"");

    }
  }

  private static Callable<Long> createTask(String host, int port, String serviceName, String groupByFieldName, String outStatisitcs, String boundingBox) {
    Callable<Long> task = () -> {
      FeatureService featureService = new FeatureService(host, port, serviceName);
      return featureService.doGroupByStats("1=1", groupByFieldName, outStatisitcs, boundingBox);
    };
    return task;
  }

  private static String generateRandomBoundingBox()  {
    double width = 180.0;
    double height = 90.0;

    double minx = random.nextDouble() * 180.0;
    minx = minx <= 0.0? minx : (-1) * minx;
    double miny = random.nextDouble() * 90.0;
    miny = miny <= 0.0? miny: (-1) * miny;

    return minx +"," + miny + "," + (minx+width) + "," + (miny + height);
  }

  private static void concurrentTesting(String serviceName, int numbThreads, int numbConcurrentCalls, String groupByFieldName, String outStatistics) {
    ExecutorService executor = Executors.newFixedThreadPool(numbThreads);

    String host = "localhost";
    int port = 9000;

    List<Callable<Long>> callables = new LinkedList<>();

    try {
      for (int index=0; index < numbConcurrentCalls; index++) {
        String boundingBox = generateRandomBoundingBox();
        callables.add(createTask(host, port, serviceName, groupByFieldName, outStatistics, boundingBox));
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
