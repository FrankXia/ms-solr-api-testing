package com.esri.arcgis.dse.test;

public class MapServiceTester {

  public static void main(String[] args) {
    testAll();
  }

  private static void figureOutBBox(String args[]) {

    String host = "localhost";
    int port = 9000;
    String name = args[0];
    String bbox = args[1];
    MapService mapService = new MapService(host, port, name);
    mapService.exportMap(bbox, 4326);
  }

  private static void testAll() {
    String host = "localhost";
    int port = 9000;

    String[] serviceNames = new String[]{"faa10k", "faa100k", "faa1m", "faa3m", "faa5m", "faa10m", "faa30m"};
    String[] bboxes = new String[serviceNames.length];
    bboxes[0] = "-180,-90,180,90";            // 10000, features returned
    bboxes[1] = "-41,-50,30,30";              //  9911
    bboxes[2] = "30,10,46.8,30.5";            //  9917
    bboxes[3] = "40.05,10.15,46.59,30.58";    //  9967
    bboxes[4] = "42.55,10.25,46.45,30.58";    //  9975
    bboxes[5] = "42.5,21.1,46.5,30.6";        //  9960
    bboxes[6] = "44.5,25,46.5,30.6";          //  9923

    for (int index =0; index < serviceNames.length; index++) {
      String name = serviceNames[index];
      MapService mapService = new MapService(host, port, name);
      mapService.exportMap(bboxes[index], 4326);
    }
  }
}
