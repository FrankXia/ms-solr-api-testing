package com.esri.arcgis.datastore.test;

import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.JSONArray;
import org.json.JSONObject;

import java.net.URLEncoder;
import java.util.*;

public class FeatureService {

  private Random random = new Random();

  private HttpClient httpClient = HttpClientBuilder.create().build();
  private String host;
  private int port;
  private String serviceName;
  private String keyspace = "esri_ds_data";

  // 36 parameters
  private String where = "";
  private String objectIds = "";
  private String time = "";
  private String geometry = "";
  private String geometryType = "esriGeometryEnvelope";
  private String geohash = "";
  private String inSR = "";
  private String spatialRel = "esriSpatialRelIntersects";
  private String distance = "";
  private String units = "esriSRUnit_Foot";
  private String relationParam = "";
  private String outFields = "";
  private boolean returnGeometry = true;
  private String maxAllowableOffset ="";
  private String geometryPrecision = "";
  private String outSR = "";
  private String gdbVersion = "";
  private boolean returnDistinctValues = false;
  private boolean returnIdsOnly = false;
  private boolean returnCountOnly = false;
  private boolean returnExtentOnly = false;
  private String orderByFields = "";
  private String groupByFieldsForStatistics = "";
  private String outStatistics = "";
  private boolean returnZ = false;
  private boolean returnM = false;
  private String multipatchOption = "";
  private String resultOffset = "";
  private int resultRecordCount = 10000;
  private String lod = "";
  private String lodType = "square";
  private String lodSR = "";
  private String timeInterval = "";
  private String timeUnit = "minutes";
  private boolean returnClusters = false;
  private boolean returnFullLodGrid = false;
  private String f = "json";

  private int timeoutInSeconds = 60;

  FeatureService(String host, int port, String serviceName, int timeoutInSeconds) {
    this.host = host;
    this.port = port;
    this.serviceName = serviceName;
    this.outFields = "*";

    RequestConfig.Builder requestBuilder = RequestConfig.custom();
    requestBuilder.setConnectTimeout(timeoutInSeconds * 1000);
    requestBuilder.setSocketTimeout(timeoutInSeconds * 1000);
    requestBuilder.setConnectionRequestTimeout(timeoutInSeconds * 1000);

    HttpClientBuilder builder = HttpClientBuilder.create();
    builder.setDefaultRequestConfig(requestBuilder.build());
//    builder.disableAutomaticRetries();
    httpClient = builder.build();

    this.timeoutInSeconds = timeoutInSeconds;
  }

  Tuple getCount(String where) {
    resetParameters2InitialValues();
    this.where = where == null? "" : where.trim();
    return getCount();
  }

  Tuple getCount(String where, String boundingBox) {
    resetParameters2InitialValues();
    this.geometry = boundingBox;
    this.where = where == null? "" : where.trim();
    return getCount();
  }

  Tuple getCount() {
    this.returnCountOnly = true;
    long totalCount = 0L;
    long start = System.currentTimeMillis();
    String queryParameters = composeGetRequestQueryParameters();
    String response = executeRequest(queryParameters);
    if (response != null) {
      System.out.println(response);
      JSONObject obj = new JSONObject(response);
      if (obj.optJSONObject("error") == null) {
        totalCount = obj.getLong("count");
      } else {
        System.out.print("Request failed -> " + response);
      }
    }
    return new Tuple(System.currentTimeMillis() - start, totalCount);
  }

  Tuple getFeaturesWithWhereClauseAndBoundingBoxAndTimeExtent(String where, String boundingBox, String timeExtent) {
    resetParameters2InitialValues();
    this.where = where == null? "" : where.trim();
    this.geometry = boundingBox;
    this.time = timeExtent;
    return getFeatures(false);
  }

  Tuple getFeaturesWithWhereClauseAndBoundingBoxAndTimeExtentAndGroupBy(String where, String boundingBox, String timeExtent, int lod, int timeInterval, String timeUnits) {
    resetParameters2InitialValues();
    this.where = where == null? "" : where.trim();
    this.geometry = boundingBox;
    this.time = timeExtent;
    this.timeInterval = timeInterval + "";
    this.timeUnit = timeUnits;
    this.lod = lod + "";
    return getFeatures(true);
  }

  Tuple getFeaturesWithWhereClauseAndRandomOffset(String where, boolean takeOffset) {
    resetParameters2InitialValues();
    this.where = where == null? "" : where.trim();
    // add random number of records skipped
    if (takeOffset) {
      int skip = random.nextInt(this.resultRecordCount / 2);
      this.resultOffset = skip + "";
    } else {
      this.resultOffset = "0";
    }
    return getFeatures(false);
  }

  Tuple getFeaturesWithWhereClauseAndBoundingBox(String where, String boundingBox) {
    resetParameters2InitialValues();
    this.where = where == null? "" : where.trim();
    this.geometry = boundingBox;
    return getFeatures(false);
  }

  Tuple getFeaturesWithTimeExtent(String where, String timeString) { // such as 1547480515000, 1547480615000
    resetParameters2InitialValues();
    this.where = where == null? "" : where.trim();
    this.time = timeString;
    return getFeatures(false);
  }

  Tuple getFeaturesWithWhereClause(String where) {
    resetParameters2InitialValues();
    this.where = where == null? "" : where.trim();
    return getFeatures(false);
  }

  Tuple doGroupByStats(String where, String groupByFdName, String outStats, String boundingBox) {
    resetParameters2InitialValues();
    this.where = where == null? "" : where.trim();
    this.groupByFieldsForStatistics = groupByFdName;
    this.outStatistics = outStats;
    if (boundingBox != null) this.geometry = boundingBox;
    return getFeatures(false);
  }

  private Tuple getFeatures(boolean isSpaceTime) {
    long start = System.currentTimeMillis();
    String queryParameters = composeGetRequestQueryParameters();
    String response = executeRequest(queryParameters);
    long numFeatures = 0;
    if (response != null) {
      //System.out.println(response);
      JSONObject obj = new JSONObject(response);
      if (obj.optJSONObject("error") == null) {
        boolean exceededTransferLimit = obj.optBoolean("exceededTransferLimit");
        JSONArray features = null;
        JSONObject spaceTimeFeatures = null;
        if (isSpaceTime) {
          spaceTimeFeatures =  obj.getJSONObject("spaceTimeFeatures");
          if (spaceTimeFeatures != null) {
            Map<String, Object> stFeatures = spaceTimeFeatures.toMap();
            for (String key : stFeatures.keySet()) {
              System.out.println(key + " -> " + stFeatures.get(key));
            }
          }
        } else {
          features = obj.getJSONArray("features");
          if (features.length() > 0) {
            // print a random feature
            int index = random.nextInt() % features.length();
            index = index < 0 ? (-1) * index : index;
            System.out.println(features.get(index));
          }
          numFeatures = features.length();
          System.out.println("# of features returned: " + features.length() + ", exceededTransferLimit: " + exceededTransferLimit + ", offset: " + this.resultOffset);
        }
      } else {
        System.out.print("Request failed -> " + response);
      }
    }
    return new Tuple(System.currentTimeMillis() - start, numFeatures);
  }

  private String composeGetRequestQueryParameters() {
    StringBuilder request = new StringBuilder();
    request.append("where=").append(URLEncoder.encode(where));
    request.append("&objectIds=").append(objectIds);
    request.append("&time=").append(URLEncoder.encode(time));
    request.append("&geometry=").append(geometry);
    request.append("&geometryType=").append(geometryType);
    request.append("&geohash=").append(geohash);
    request.append("&inSR=").append(inSR);
    request.append("&spatialRel=").append(spatialRel);
    request.append("&distance=").append(distance);
    request.append("&units=").append(units);
    request.append("&relationParam=").append(relationParam);
    request.append("&outFields=").append(outFields);
    request.append("&returnGeometry=").append(returnGeometry);
    request.append("&maxAllowableOffset=").append(maxAllowableOffset);
    request.append("&geometryPrecision=").append(geometryPrecision);
    request.append("&outSR=").append(outSR);
    request.append("&gdbVersion=").append(gdbVersion);
    request.append("&returnDistinctValues=").append(returnDistinctValues);
    request.append("&returnIdsOnly=").append(returnIdsOnly);
    request.append("&returnCountOnly=").append(returnCountOnly);
    request.append("&returnExtentOnly=").append(returnExtentOnly);
    request.append("&orderByFields=").append(orderByFields);
    request.append("&groupByFieldsForStatistics=").append(groupByFieldsForStatistics);
    request.append("&outStatistics=").append(URLEncoder.encode(outStatistics));
    request.append("&returnZ=").append(returnZ);
    request.append("&returnM=").append(returnM);
    request.append("&multipatchOption=").append(multipatchOption);
    request.append("&resultOffset=").append(resultOffset);
    request.append("&resultRecordCount=").append(resultRecordCount);
    request.append("&lod=").append(lod);
    request.append("&lodType=").append(lodType);
    request.append("&lodSR=").append(lodSR);
    request.append("&timeInterval=").append(timeInterval);
    request.append("&timeUnit=").append(timeUnit);
    request.append("&returnClusters=").append(returnClusters);
    request.append("&returnFullLodGrid=").append(returnFullLodGrid);
    request.append("&f=").append(f);

    return request.toString();
  }

  private void resetParameters2InitialValues() {
    this.where = "";
    this.objectIds = "";
    this.time = "";
    this.geometry = "";
    this.geometryType = "esriGeometryEnvelope";
    this.geohash = "";
    this.inSR = "";
    this.spatialRel = "esriSpatialRelIntersects";
    this.distance = "";
    this.units = "esriSRUnit_Foot";
    this.relationParam = "";
    this.outFields = "*";
    this.returnGeometry = true;
    this.maxAllowableOffset ="";
    this.geometryPrecision = "";
    this.outSR = "";
    this.gdbVersion = "";
    this.returnDistinctValues = false;
    this.returnIdsOnly = false;
    this.returnCountOnly = false;
    this.returnExtentOnly = false;
    this.orderByFields = "";
    this.groupByFieldsForStatistics = "";
    this.outStatistics = "";
    this.returnZ = false;
    this.returnM = false;
    this.multipatchOption = "";
    this.resultOffset = "";
    this.resultRecordCount = 10000;
    this.lod = "";
    this.lodType = "square";
    this.lodSR = "";
    this.timeInterval = "";
    this.timeUnit = "minutes";
    this.returnClusters = false;
    this.returnFullLodGrid = false;
    this.f = "json";
  }

  private String executeRequest(String queryParameters) {
    HttpClient client = httpClient;
    try {
      String url = "http://" + host + ":" + port + "/arcgis/rest/services/" + serviceName + "/FeatureServer/0/query?" + queryParameters;
      System.out.println(url);
      //long start = System.currentTimeMillis();
      String result = Utils.executeHttpGET(client, url);
      //System.out.println("======> Total request time: " + (System.currentTimeMillis() - start)  + " ms, service name: " + serviceName);
      return result;
    } catch (Exception ex) {
      ex.printStackTrace();
    }
    return null;
  }

  JSONObject getFieldStats(String fieldName) {
    String stats = "[" +
        "{\"statisticType\":\"min\",\"onStatisticField\":\"" + fieldName + "\",\"outStatisticFieldName\":\"min\"}," +
        "{\"statisticType\":\"max\",\"onStatisticField\":\"" + fieldName + "\",\"outStatisticFieldName\":\"max\"}]";
    this.outStatistics = stats;
    this.where = "1=1";
    this.resultRecordCount = 100000;
    //long start = System.currentTimeMillis();
    String queryParameters = composeGetRequestQueryParameters();
    String response = executeRequest(queryParameters);
    JSONObject jsonObject = new JSONObject(response);
    System.out.println(response);
    JSONObject f = jsonObject.getJSONArray("features").getJSONObject(0);
    return f.getJSONObject("attributes");
  }

  List<String> getFieldUniqueValues(String fieldName) {
    List<String> uniqueValues = new LinkedList<String>();

    //this.groupByFieldsForStatistics = fieldName;
    //this.outStatistics = "[{\"statisticType\":\"count\",\"onStatisticField\":\"" + fieldName + "\",\"outStatisticFieldName\":\"count\"}]";
    this.where = "1=1";
    this.returnDistinctValues = true;
    this.outFields = fieldName;
    this.resultRecordCount = 100000;
    this.orderByFields = fieldName;
    String queryParameters = composeGetRequestQueryParameters();
    String response = executeRequest(queryParameters);
    JSONObject jsonObject = new JSONObject(response);
    JSONArray features = jsonObject.getJSONArray("features");
    for (int i=0; i<features.length(); i++) {
      JSONObject f = features.getJSONObject(i);
      uniqueValues.add(f.getJSONObject("attributes").get(fieldName).toString());
    }
    System.out.println(response);
    return uniqueValues;
  }

//
//  Solr related functions
//
//  JSONObject getStates(String fieldName) {
//    int solrPort = 8983;
//    HttpClient client = httpClient;
//    try {
//      String queryString = "q=*:*&useFieldCache=true&stats=true&stats.field=" + fieldName + "&wt=json&rows=0";
//      String url = "http://" + host + ":" + solrPort + "/solr/" + keyspace +"." + serviceName + "/select?" + queryString;
//      System.out.println(url);
//
//      String jsonString = Utils.executeHttpGET(client, url);
//
//      JSONObject jsonObject = new JSONObject(jsonString);
//      return jsonObject.getJSONObject("stats").getJSONObject("stats_fields").getJSONObject(fieldName);
//    } catch (Exception ex) {
//      ex.printStackTrace();
//    }
//    return null;
//  }
//
//  List<String> getUniqueValues(String fieldName) {
//    int solrPort = 8983;
//    HttpClient client = httpClient;
//    List<String> uniqueValues = new LinkedList<String>();
//    try {
//      int maxReturns = 100;
//      String queryString = "q=*:*&useFieldCache=true&json.facet={" + fieldName + ":{type:terms,field:" + fieldName + ",limit:" + maxReturns + "}}&wt=json&rows=0";
//      String url = "http://" + host + ":" + solrPort + "/solr/" + keyspace +"." + serviceName + "/select?" + queryString.replaceAll("[{]", "%7B").replaceAll("[}]", "%7D");
//      System.out.println(url);
//      System.out.println(url.substring(90));
//
//      String jsonString = Utils.executeHttpGET(client, url);
//
//      JSONObject jsonObject = new JSONObject(jsonString);
//      JSONArray buckets = jsonObject.getJSONObject("facets").getJSONObject(fieldName).getJSONArray("buckets");
//
//      for (int i=0; i<buckets.length(); i++) {
//        JSONObject bucket = buckets.getJSONObject(i);
//        String key = bucket.get("val").toString();
//        uniqueValues.add(key);
//      }
//    } catch (Exception ex) {
//      ex.printStackTrace();
//    }
//    return uniqueValues;
//  }

}
