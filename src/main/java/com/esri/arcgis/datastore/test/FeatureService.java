package com.esri.arcgis.datastore.test;

import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.JSONArray;
import org.json.JSONObject;

import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

public class FeatureService {

  private Random random = new Random();

  private HttpClient httpClient = HttpClientBuilder.create().build();
  private String host;
  private int port;
  private String serviceName;
  private String keyspace = "esri_ds_data";
  private Dialect dialect = Dialect.solr;

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
    try{
      //test PG connection
      int pgPort = 5432;
      String url = "jdbc:postgresql://a21:" + pgPort + "/realtime";
      Properties properties = new Properties();
      properties.put("user", "realtime");
      properties.put("password", "esri.test");
      Class.forName("org.postgresql.Driver");
      Connection  connection = DriverManager.getConnection(url, properties);
      connection.close();

      this.dialect = Dialect.sql;
    } catch (Exception e){
      e.printStackTrace();
      this.dialect = Dialect.solr;
    }
  }

  long getCount(String where) {
    resetParameters2InitialValues();
    this.where = where == null? "" : where.trim();
    return getCount();
  }

  long getCount(String where, String boundingBox) {
    resetParameters2InitialValues();
    this.geometry = boundingBox;
    this.where = where == null? "" : where.trim();
    return getCount();
  }

  private long getCount() {
    this.returnCountOnly = true;
    long totalCount = 0L;
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
    return totalCount;
  }

  void getFeaturesWithWhereClauseAndBoundingBoxAndTimeExtent(String where, String boundingBox, String timeExtent) {
    resetParameters2InitialValues();
    this.where = where == null? "" : where.trim();
    this.geometry = boundingBox;
    this.time = timeExtent;
    getFeatures();
  }

  void getFeaturesWithWhereClauseAndRandomOffset(String where, boolean takeOffset) {
    resetParameters2InitialValues();
    this.where = where == null? "" : where.trim();
    // add random number of records skipped
    if (takeOffset) {
      int skip = random.nextInt(this.resultRecordCount / 2);
      this.resultOffset = skip + "";
    } else {
      this.resultOffset = "0";
    }
    getFeatures();
  }

  void getFeaturesWithWhereClauseAndBoundingBox(String where, String boundingBox) {
    resetParameters2InitialValues();
    this.where = where == null? "" : where.trim();
    this.geometry = boundingBox;
    getFeatures();
  }

  void getFeaturesWithTimeExtent(String where, String timeString) { // such as 1547480515000, 1547480615000
    resetParameters2InitialValues();
    this.where = where == null? "" : where.trim();
    this.time = timeString;
    getFeatures();
  }

  void getFeaturesWithWhereClause(String where) {
    resetParameters2InitialValues();
    this.where = where == null? "" : where.trim();
    getFeatures();
  }

  Tuple doGroupByStats(String where, String groupByFdName, String outStats, String boundingBox) {
    resetParameters2InitialValues();
    this.where = where == null? "" : where.trim();
    this.groupByFieldsForStatistics = groupByFdName;
    this.outStatistics = outStats;
    if (boundingBox != null) this.geometry = boundingBox;
    return getFeatures();
  }

  private Tuple getFeatures() {
    long start = System.currentTimeMillis();
    String queryParameters = composeGetRequestQueryParameters();
    String response = executeRequest(queryParameters);
    long numFeatures = 0;
    if (response != null) {
      //System.out.println(response);
      JSONObject obj = new JSONObject(response);
      if (obj.optJSONObject("error") == null) {
        boolean exceededTransferLimit = obj.optBoolean("exceededTransferLimit");
        JSONArray features = obj.getJSONArray("features");
        if (features.length() > 0) {
          // print a random feature
          int index  = random.nextInt() % features.length();
          index = index < 0 ? (-1) * index : index;
          System.out.println(features.get(index));
        }
        numFeatures = features.length();
        System.out.println("# of features returned: " + features.length() + ", exceededTransferLimit: " + exceededTransferLimit + ", offset: " + this.resultOffset);
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
      long start = System.currentTimeMillis();
      String result = Utils.executeHttpGET(client, url);
      System.out.println("======> Total request time: " + (System.currentTimeMillis() - start)  + " ms, service name: " + serviceName);
      return result;
    } catch (Exception ex) {
      ex.printStackTrace();
    }
    return null;
  }

//
//  Solr related functions
//

  HashMap<String,String> getStats(String fieldName) {

    if(dialect == Dialect.sql){
      return getPGStats(fieldName);
    } else {
      return getSolrStats(fieldName);
    }
  }

  List<String> getUniqueValues(String fieldName){
    if(dialect == Dialect.sql){
      return getPGUniqueValues(fieldName);
    } else {
      return getSolrUniqueValues(fieldName);
    }
  }

  List<String> getPGUniqueValues(String fieldName){
    int pgPort = 5432;
    String url = "jdbc:postgresql://a21:" + pgPort + "/realtime";
    Properties properties = new Properties();
    properties.put("user", "realtime");
    properties.put("password", "esri.test");

    Connection connection = null;
    Statement statement = null;
    try {
      Class.forName("org.postgresql.Driver");
      connection = DriverManager.getConnection(url, properties);
      statement = connection.createStatement();

      StringBuilder sql = new StringBuilder();
      sql.append("SELECT distinct(").append(fieldName).append(")").append(" FROM ").append(keyspace).append(".").append(serviceName);
      ResultSet resultSet = statement.executeQuery(sql.toString());

      List<String> stats = new ArrayList<>();
      while(resultSet.next()){
        stats.add(resultSet.getString(1));
      }
      return stats;
    }
    catch (Exception e){
      e.printStackTrace();
    }
    finally {
      try{
        statement.close();
        connection.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return new ArrayList<>();
  }


  HashMap<String, String> getPGStats(String fieldName){
    int pgPort = 5432;
    String url = "jdbc:postgresql://a21:" + pgPort + "/realtime";
    Properties properties = new Properties();
    properties.put("user", "realtime");
    properties.put("password", "esri.test");

    Connection connection = null;
    Statement statement = null;
    try {
      Class.forName("org.postgresql.Driver");
      connection = DriverManager.getConnection(url, properties);
      statement = connection.createStatement();

      StringBuilder sql = new StringBuilder();
      sql.append("SELECT min(").append(fieldName).append(") as min").append(", max(").append(fieldName)
          .append(") as max FROM ").append(keyspace).append(".").append(serviceName);
      ResultSet resultSet = statement.executeQuery(sql.toString());
      if(resultSet.next()){
        HashMap<String, String> stats = new HashMap<String, String>();
        stats.put("min", resultSet.getString("min"));
        stats.put("max", resultSet.getString("max"));
        return stats;
      }
    }
    catch (Exception e){
      e.printStackTrace();
    }
    finally {
      try{
        statement.close();
        connection.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return new HashMap<>();
  }


  HashMap<String, String> getSolrStats(String fieldName) {

    int solrPort = 8983;
    HttpClient client = httpClient;
    try {
      String queryString = "q=*:*&useFieldCache=true&stats=true&stats.field=" + fieldName + "&wt=json&rows=0";
      String url = "http://" + host + ":" + solrPort + "/solr/" + keyspace +"." + serviceName + "/select?" + queryString;
      System.out.println(url);

      String jsonString = Utils.executeHttpGET(client, url);

      JSONObject jsonObject = new JSONObject(jsonString);
      JSONObject stats = jsonObject.getJSONObject("stats").getJSONObject("stats_fields").getJSONObject(fieldName);

      String min = stats.getString("min");
      String max = stats.getString("max");

      HashMap<String, String> statsMap = new HashMap<>();
      statsMap.put("min", min);
      statsMap.put("max", max);
      return statsMap;
    } catch (Exception ex) {
      ex.printStackTrace();
    }
    return new HashMap<>();
  }

  List<String> getSolrUniqueValues(String fieldName) {
    int solrPort = 8983;
    HttpClient client = httpClient;
    List<String> uniqueValues = new LinkedList<String>();
    try {
      int maxReturns = 100;
      String queryString = "q=*:*&useFieldCache=true&json.facet={" + fieldName + ":{type:terms,field:" + fieldName + ",limit:" + maxReturns + "}}&wt=json&rows=0";
      String url = "http://" + host + ":" + solrPort + "/solr/" + keyspace +"." + serviceName + "/select?" + queryString.replaceAll("[{]", "%7B").replaceAll("[}]", "%7D");
      System.out.println(url);
      System.out.println(url.substring(90));

      String jsonString = Utils.executeHttpGET(client, url);

      JSONObject jsonObject = new JSONObject(jsonString);
      JSONArray buckets = jsonObject.getJSONObject("facets").getJSONObject(fieldName).getJSONArray("buckets");

      for (int i=0; i<buckets.length(); i++) {
        JSONObject bucket = buckets.getJSONObject(i);
        String key = bucket.get("val").toString();
        uniqueValues.add(key);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
    }
    return uniqueValues;
  }

}

enum Dialect {
  sql, solr
}
