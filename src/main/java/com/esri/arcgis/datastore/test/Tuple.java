package com.esri.arcgis.datastore.test;

public class Tuple {
  long requestTime;
  long returnedFeatures;

  public Tuple(long requestTime, long returnedFeatures) {
    this.requestTime = requestTime;
    this.returnedFeatures = returnedFeatures;
  }
}
