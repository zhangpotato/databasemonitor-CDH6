package com.longdb.cm;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.longdb.connect.PropertiesInfo;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author hongtao
 */
public class HDFSStatus {
    public static final String CLUSTER_NAME = PropertiesInfo.getConfProperty().getProperty("Cluster_Name");
    //hdfs
    public static String getHDFSReplication(String cmHost, int cmPort, String cmUsername, String cmPassword) throws IOException {
        String url = "http://"+cmHost+":"+cmPort+"/api/v33/clusters/"+CLUSTER_NAME+"/services/hdfs/config?view=full";
        HttpGet request = new HttpGet(url);
        String auth = cmUsername + ":" + cmPassword;
        byte[] encodedAuth = Base64.encodeBase64(
                auth.getBytes(StandardCharsets.UTF_8));
        String authHeader = "Basic " + new String(encodedAuth);
        request.setHeader(HttpHeaders.AUTHORIZATION, authHeader);
        CloseableHttpClient client = HttpClientBuilder.create().build();
        CloseableHttpResponse response = client.execute(request);

        JSONObject json = JSONObject.parseObject(EntityUtils.toString(response.getEntity()));
        JSONArray jsonArray = json.getJSONArray("items");
        String replication = null;
        for (int i = 0; i < jsonArray.size(); i++) {
            if ("dfs.replication".equals(jsonArray.getJSONObject(i).getString("relatedName"))) {
                replication = jsonArray.getJSONObject(i).getString("default");
            } else {
                continue;
            }
        }
        client.close();
        return replication;
    }
}
