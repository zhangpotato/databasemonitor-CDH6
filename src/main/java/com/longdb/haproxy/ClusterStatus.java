package com.longdb.haproxy;

import com.alibaba.fastjson.JSONArray;
import com.longdb.connect.ConnectInfo;
import com.longdb.connect.PropertiesInfo;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.HashMap;

/**
 * @author hongtao
 */
public class ClusterStatus {
    public static final String HAPROXY_URL = PropertiesInfo.getConfProperty().getProperty("HAProxyUrl");
    public static final String USERNAME = PropertiesInfo.getConfProperty().getProperty("HAProxyUsername");
    public static final String PASSWORD = PropertiesInfo.getConfProperty().getProperty("HAProxyPassword");
    //haproxy
    public static HashMap getCurrentClusterStatus() {
        CloseableHttpResponse httpResponse = null;
        JSONArray json = null;

        HashMap<String, Integer> clusterStatus = new HashMap<>();
        try {
            httpResponse = ConnectInfo.connectWithBasicAUTH(HAPROXY_URL, "/haproxy?stats;json", USERNAME, PASSWORD);
            json = JSONArray.parseArray(EntityUtils.toString(httpResponse.getEntity()));
            for (int i = 0; i < json.size(); i++) {
                JSONArray jsonArray = json.getJSONArray(i);
//            判断是否包含objType = Backend
                Boolean flag1 = "Backend".equals(jsonArray.getJSONObject(0).get("objType"));
//            判断是否包含value = longdb-cluster
                Boolean flag2 = jsonArray.getJSONObject(0).getJSONObject("value").containsValue(PropertiesInfo.getConfProperty().getProperty("HAProxy_Monitor_key"));
//
                if (flag1 && flag2) {
//                满足条件输出当前存活的regionserver数量
                    int currentRegionServerCount = Integer.parseInt(jsonArray.getJSONObject(18).getJSONObject("value").get("value").toString());
                    int currentSessionsCount = Integer.parseInt(jsonArray.getJSONObject(4).getJSONObject("value").get("value").toString());
                    clusterStatus.put("currentRegionServerCount", currentRegionServerCount);
                    clusterStatus.put("currentSessionsCount", currentSessionsCount);
                }
            }
        } catch (IOException ioException) {
            clusterStatus.put("currentRegionServerCount", 0);
            clusterStatus.put("currentSessionsCount", 0);
            ioException.printStackTrace();
        }

        return clusterStatus;
    }

}
