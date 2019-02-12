package com.esri.arcgis.dse.test;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URLEncoder;

public class MapService {

  private int dpi = 96;
  private boolean transparent = true;
  private String format = "png";
  private String sizeString = "512,512";
  private String bbox = "385242.6225571957,-157154.53015433898,1006522.7884590775,243986.99428624607";
  private int bboxSR = 102100;
  private int imageSR = 102100;
  private String f = "image";
  private String dynamicLayers = "";

  private int featureLimit = 10000;


  private HttpClient httpClient = HttpClientBuilder.create().build();
  private String host;
  private int port;
  private String serviceName;

  public MapService(String host, int port, String serviceName) {
    this.host = host;
    this.port = port;
    this.serviceName = serviceName;
  }

  public void exportMap(String bbox, int bboxSR) {
    this.bbox = bbox;
    this.bboxSR = bboxSR;

    StringBuilder queryParameters = new StringBuilder();
    queryParameters.append("dpi=").append(dpi);
    queryParameters.append("&transparent=").append(transparent);
    queryParameters.append("&format=").append(format);
    queryParameters.append("&dynamicLayers=").append(URLEncoder.encode(createDynamicLayers(featureLimit, serviceName)));
    queryParameters.append("&bbox=").append(URLEncoder.encode(bbox));
    queryParameters.append("&bboxSR=").append(bboxSR);
    queryParameters.append("&imageSR=").append(imageSR);
    queryParameters.append("&size=").append(URLEncoder.encode(sizeString));
    queryParameters.append("&f=").append(f);

    String response = executeMapExportRequest(queryParameters.toString());
    if (response==null) {
      System.out.println("?????? failed to export image!");
    }
    System.out.println("Export image succeeded!");
  }

  private String createDynamicLayers(int featureLimit, String layerName) {
    String template = "[{\"id\":0,\"name\":\"" + layerName + "\",\"source\":{\"type\":\"mapLayer\",\"mapLayerId\":0},\"drawingInfo\":{\"renderer\":{\"type\":\"aggregation\",\"style\":\"Grid\",\"featureThreshold\":" + featureLimit +  ",\"lodOffset\":0,\"minBinSizeInPixels\":25,\"fullLodGrid\":false,\"labels\":{\"color\":[0,0,0,255],\"font\":\".SF NS Text\",\"size\":12,\"style\":\"PLAIN\",\"format\":\"###.#KMB\"},\"fieldStatistic\":null,\"binRenderer\":{\"type\":\"Continuous\",\"minColor\":[255,0,0,0],\"maxColor\":[255,0,0,255],\"minOutlineColor\":[0,0,0,100],\"maxOutlineColor\":[0,0,0,100],\"minOutlineWidth\":0.5,\"maxOutlineWidth\":0.5,\"minValue\":null,\"maxValue\":null,\"minSize\":100,\"maxSize\":100,\"normalizeByBinArea\":false},\"geoHashStyle\":{\"style\":\"geohash\",\"sr\":\"102100\"},\"featureRenderer\":{\"type\":\"simple\",\"symbol\":{\"type\":\"esriSMS\",\"style\":\"esriSMScircle\",\"color\":[158,202,225,150],\"size\":12,\"angle\":0,\"xoffset\":0,\"yoffset\":0,\"outline\":{\"color\":[0,0,0,255],\"width\":1}},\"label\":\"\",\"description\":\"\",\"rotationType\":\"\",\"rotationExpression\":\"\"}}},\"minScale\":0,\"maxScale\":0}]";
    return template;
  }

  private String executeMapExportRequest(String queryParameters) {
    HttpClient client = httpClient;
    try {
      String url = "http://" + host + ":" + port + "/arcgis/rest/services/" + serviceName + "/MapServer/export?" + queryParameters;
      System.out.println(url);

      long start = System.currentTimeMillis();
      HttpGet request = new HttpGet(url);
      HttpResponse response = client.execute(request);
      System.out.println("Response Code : " + response.getStatusLine().getStatusCode());

      BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
      StringBuilder result = new StringBuilder();
      String line = "";
      while ((line = rd.readLine()) != null) {
        result.append(line);
      }

      System.out.println("======> Total request time: " + (System.currentTimeMillis() - start)  + " ms, service name: " + serviceName);
      return result.toString();
    } catch (Exception ex) {
      ex.printStackTrace();
    }
    return null;
  }

  long getCount(String where, String boundingBox) {
    FeatureService featureService = new FeatureService(host, port, serviceName);
    return featureService.getCount(where, boundingBox);
  }
}
