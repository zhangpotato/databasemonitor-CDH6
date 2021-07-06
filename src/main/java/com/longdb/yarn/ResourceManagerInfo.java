package com.longdb.yarn;

import com.longdb.connect.PropertiesInfo;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @author hongtao
 */
public class ResourceManagerInfo {
    public static ArrayList<String> resourceManagerInfo=new ArrayList();
    public static final String ResourceManager1_URL = "http://" + PropertiesInfo.getConfProperty().getProperty("ResourceManager1") + "/cluster";
    public static final String ResourceManager2_URL = "http://" + PropertiesInfo.getConfProperty().getProperty("ResourceManager2") + "/cluster";

    public static String getActiveResourceManagerUrl() {
        int statusCode = 0;
        try {
            statusCode = Jsoup.connect(ResourceManager1_URL).execute().statusCode();
        } catch (IOException e) {
            e.printStackTrace();
        }
        String url = 200 == statusCode ? ResourceManager1_URL : ResourceManager2_URL;
        return url;
    }

    public static void addMetrics(Elements elements) {
        for (Element element : elements) {
            for (int i = 0; i < element.child(0).childrenSize(); i++) {
                if (element.child(0).child(i).text().split(" ").length > 1) {
                    resourceManagerInfo.add(element.child(0).child(i).text().split(" ")[0].trim());
                } else {
                    resourceManagerInfo.add(element.child(0).child(i).text().trim());
                }
            }
        }
    }

    public static ArrayList<String> getResourceManagerInfo(String url) {
        Document doc = null;
        try {
            doc = Jsoup.connect(url).get();
        } catch (IOException e) {
            e.printStackTrace();
        }
        //Cluster Metrics
        Elements clusterMetrics = doc.getElementById("metricsoverview").getElementsByClass("ui-widget-content");
        ResourceManagerInfo.addMetrics(clusterMetrics);
        //Cluster Nodes Metrics
        Elements nodesMetrics = doc.getElementById("nodemetricsoverview").getElementsByClass("ui-widget-content");
        ResourceManagerInfo.addMetrics(nodesMetrics);
        //User Metrics for dr.who
        Elements userMetrics = doc.getElementById("usermetricsoverview").getElementsByClass("ui-widget-content");
        ResourceManagerInfo.addMetrics(userMetrics);
        return resourceManagerInfo;
    }


}
