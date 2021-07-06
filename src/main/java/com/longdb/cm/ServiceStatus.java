package com.longdb.cm;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.longdb.connect.ConnectInfo;
import com.longdb.connect.PropertiesInfo;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;

import java.io.IOException;
import java.util.HashMap;

/**
 * @author hongtao
 */
public class ServiceStatus {
    public static HashMap<String, Integer> getServiceStatus() {
        HashMap<String, Integer> serviceStatus = new HashMap<>();
        int entityStatus = 0;
        String baseUrl = "http://" + PropertiesInfo.getConfProperty().getProperty("cmHost") + ":"
                + PropertiesInfo.getConfProperty().getProperty("cmPort");
        String path = "/api/v33/clusters/" + PropertiesInfo.getConfProperty().getProperty("Cluster_Name") + "/services";
        CloseableHttpResponse response = ConnectInfo.connectWithBasicAUTH(baseUrl, path, PropertiesInfo.getConfProperty().getProperty("cmUsername"), PropertiesInfo.getConfProperty().getProperty("cmPassword"));
        JSONArray json = null;

        try {
            json = JSONObject.parseObject(EntityUtils.toString(response.getEntity())).getJSONArray("items");
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < json.size(); i++) {
            if ("GOOD_HEALTH".equals(json.getJSONObject(i).get("entityStatus"))) {
                entityStatus = 1;
            } else if ("STOPPED".equals(json.getJSONObject(i).get("entityStatus"))) {
                entityStatus = -1;
            } else {
                entityStatus = 0;
            }
            serviceStatus.put(json.getJSONObject(i).get("type").toString(), entityStatus);
        }

        String serviceBaseUrl = "https://" + PropertiesInfo.getConfProperty().getProperty("LongDBAdmin_Address") + ":";
        ConnectInfo.disableSSLCertCheck();
        int esStatus = 0;

        try {
            esStatus = Jsoup.connect(serviceBaseUrl + PropertiesInfo.getConfProperty().getProperty("esPort")).execute().statusCode();
        } catch (IOException e) {
            e.printStackTrace();
        }

        int metabaseStatus = 0;
        try {
            metabaseStatus = Jsoup.connect(serviceBaseUrl + PropertiesInfo.getConfProperty().getProperty("metabasePort")).execute().statusCode();
        } catch (IOException e) {
            e.printStackTrace();
        }

        int notebookStatus = 0;
        try {
            notebookStatus = Jsoup.connect(serviceBaseUrl + PropertiesInfo.getConfProperty().getProperty("notebookPort")).execute().statusCode();
        } catch (IOException e) {
            e.printStackTrace();
        }

        serviceStatus.put("KIBANA", esStatus == 200 ? 1 : -1);
        serviceStatus.put("AGILEBI", metabaseStatus == 200 ? 1 : -1);
        serviceStatus.put("NOTEBOOK", notebookStatus == 200 ? 1 : -1);

        return serviceStatus;
    }
}
