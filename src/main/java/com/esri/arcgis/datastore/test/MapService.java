package com.esri.arcgis.datastore.test;

import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.HttpClientBuilder;
import java.net.URLEncoder;

public class MapService {

  private int dpi = 96;
  private boolean transparent = true;
  private String format = "png";
  private String sizeString = "750,500";
  private String bbox = "10,10,50,50";
  private int bboxSR = 4326;
  private int imageSR = 102100;
  private String f = "image";
  private String dynamicLayers = "";

  private int featureLimit = 10000;
  private String aggregationStyle = "square";

  private HttpClient httpClient;
  private String host;
  private int port;
  private String serviceName;
  private int timeoutInSeconds = 60; // seconds

  public MapService(String host, int port, String serviceName, int timeoutInSeconds) {
    this.host = host;
    this.port = port;
    this.serviceName = serviceName;

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

  public long exportMap(String bbox, int bboxSR) {
    this.bbox = bbox;
    this.bboxSR = bboxSR;

    String url = "http://" + host + ":" + port + "/arcgis/rest/services/" + serviceName + "/MapServer/export?" + getParameters();
    long response = Utils.executeHttpGETRequest(httpClient, url, serviceName);
    if (response == -1) {
      System.out.println("?????? failed to export image!");
    }
    System.out.println("Export image succeeded!");
    return response;
  }

  private String getParameters() {
    this.dynamicLayers = createDynamicLayers(featureLimit, serviceName);

    StringBuilder queryParameters = new StringBuilder();
    queryParameters.append("dpi=").append(dpi);
    queryParameters.append("&transparent=").append(transparent);
    queryParameters.append("&format=").append(format);
    queryParameters.append("&dynamicLayers=").append(URLEncoder.encode(dynamicLayers));
    queryParameters.append("&bbox=").append(URLEncoder.encode(bbox));
    queryParameters.append("&bboxSR=").append(bboxSR);
    queryParameters.append("&imageSR=").append(imageSR);
    queryParameters.append("&size=").append(URLEncoder.encode(sizeString));
    queryParameters.append("&f=").append(f);
    return  queryParameters.toString();
  }

  public long exportMap(String bbox, int bboxSR, String aggregationStyle) {
    this.bbox = bbox;
    this.bboxSR = bboxSR;
    this.aggregationStyle = aggregationStyle;

    long start = System.currentTimeMillis();
    String url = "http://" + host + ":" + port + "/arcgis/rest/services/" + serviceName + "/MapServer/export?" + getParameters();
    long response = Utils.executeHttpGETRequest(httpClient,url, serviceName);

    // System.out.println(queryParameters.toString());
    // String url = "http://" + host + ":" + port + "/arcgis/rest/services/" + serviceName + "/MapServer/export?";
    // long response = doHttpUrlConnectionAction(url, queryParameters.toString());


    if (response == -1) {
      System.out.println("?????? failed to export image! " + (System.currentTimeMillis() - start) + " ms");
      return (System.currentTimeMillis() - start);
    }
    System.out.println("Export image succeeded!");
    return response;
  }

  private String createDynamicLayers(int featureLimit, String layerName) {
    String template = "[{\"id\":0,\"name\":\"" + layerName + "\",\"source\":{\"type\":\"mapLayer\",\"mapLayerId\":0},\"drawingInfo\":" +
        "{\"renderer\":{\"type\":\"aggregation\",\"style\":\"Grid\",\"featureThreshold\":" + featureLimit +
        ",\"lodOffset\":0,\"minBinSizeInPixels\":25,\"fullLodGrid\":false,\"labels\":" +
        "{\"color\":[0,0,0,255],\"font\":\".SF NS Text\",\"size\":12,\"style\":\"PLAIN\",\"format\":\"###.#KMB\"},\"fieldStatistic\":null," +
        "\"binRenderer\":{\"type\":\"Continuous\",\"minColor\":[255,0,0,0],\"maxColor\":[255,0,0,255],\"minOutlineColor\":[0,0,0,100]," +
        "\"maxOutlineColor\":[0,0,0,100],\"minOutlineWidth\":0.5,\"maxOutlineWidth\":0.5,\"minValue\":null,\"maxValue\":null,\"minSize\":100,\"maxSize\":100,\"normalizeByBinArea\":false}," +
        "\"geoHashStyle\":{\"style\":\"" + aggregationStyle + "\"," +
        "\"sr\":\"102100\"},\"featureRenderer\":{\"type\":\"simple\",\"symbol\":{\"type\":\"esriSMS\",\"style\":\"esriSMScircle\"," +
        "\"color\":[158,202,225,150],\"size\":12,\"angle\":0,\"xoffset\":0,\"yoffset\":0,\"outline\":{\"color\":[0,0,0,255],\"width\":1}}," +
        "\"label\":\"\",\"description\":\"\",\"rotationType\":\"\",\"rotationExpression\":\"\"}}},\"minScale\":0,\"maxScale\":0}]";
    return template;
  }


  long getCount(String where, String boundingBox) {
    FeatureService featureService = new FeatureService(host, port, serviceName);
    return featureService.getCount(where, boundingBox);
  }

}
