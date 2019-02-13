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
    if (args.length == 6) {
      String host = args[0];
      String serviceName = args[1];
      int numThreads = Integer.parseInt(args[2]);
      int numCalls = Integer.parseInt(args[3]);
      String groupByFieldName = args[4];
      String outStatisitcs = args[5];
      concurrentTesting(host, serviceName, numThreads, numCalls, groupByFieldName, outStatisitcs);
    } else {
      System.out.println("Usage: java -cp ./target/ms-solr-api-performance-1.0.jar com.esri.arcgis.dse.test.FeatureServiceConcurrentTester <Host name> <Service name> <Number of threads> <Number of concurrent calls (<=100)> <Group By field name> <Out Statistics>");
      System.out.println("Sample:");
      System.out.println("   java -cp  ./target/ms-solr-api-performance-1.0.jar com.esri.arcgis.dse.test.FeatureServiceConcurrentTester localhost faa30m 4 8 dest  \"[" +
          " {\\\"statisticType\\\":\\\"avg\\\",\\\"onStatisticField\\\":\\\"speed\\\",\\\"outStatisticFieldName\\\":\\\"avg_speed\\\"}," +
          " {\\\"statisticType\\\":\\\"min\\\",\\\"onStatisticField\\\":\\\"speed\\\",\\\"outStatisticFieldName\\\":\\\"min_speed\\\"}," +
          " {\\\"statisticType\\\":\\\"max\\\",\\\"onStatisticField\\\":\\\"speed\\\",\\\"outStatisticFieldName\\\":\\\"max_speed\\\"} " +
          "]\"");

    }
  }

  private static Callable<Tuple> createTask(String host, int port, String serviceName, String groupByFieldName, String outStatisitcs, String boundingBox) {
    Callable<Tuple> task = () -> {
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

  private static void concurrentTesting(String host, String serviceName, int numbThreads, int numbConcurrentCalls, String groupByFieldName, String outStatistics) {
    ExecutorService executor = Executors.newFixedThreadPool(numbThreads);

    int port = 9000;

    List<Callable<Tuple>> callables = new LinkedList<>();

    try {
      for (int index=0; index < numbConcurrentCalls; index++) {
        String boundingBox = generateRandomBoundingBox();
        callables.add(createTask(host, port, serviceName, groupByFieldName, outStatistics, boundingBox));
      }

      Stream<Tuple> results =
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
      final List<Long> features = new LinkedList<>();
      results.forEach( tuple -> {
        times.add(tuple.requestTime);
        features.add(tuple.returnedFeatures);
      });

      long timeTotal = 0;
      long featureTotal = 0;
      for (int i=0; i<times.size(); i++) {
        timeTotal += times.get(i);
        featureTotal += features.get(i);
      }
//      long totalFinal = results.reduce(0L, (total, i) -> total + i);
//      System.out.println( (double)totalFinal / (double)callables.size());

      System.out.println( "Average time and features over " + features.size() +  " requests: " +  (double)timeTotal / (double)times.size() + " " + (double)featureTotal / (double)features.size());

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
