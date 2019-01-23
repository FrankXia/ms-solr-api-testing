package com.esri.arcgis.dse.test;

public class FeatureServiceTester {


  public static void main(String[] args) {
    FeatureService featureService = new FeatureService("localhost", 9000, "faa10k");
    long totalCount = featureService.getCount("1=1");
    System.out.println(totalCount);

    featureService.getFeaturesWithWhereClauseAndRandomOffset("1=1");

    featureService.getFeaturesWithWhereClauseAndBoundingBox("1=1", "-45,-30,45,30");
  }
}
