package com.longdb.hdfs;

import com.alibaba.fastjson.JSONObject;
import com.longdb.connect.ConnectInfo;
import com.longdb.connect.PropertiesInfo;

import java.util.ArrayList;

/**
 * @author hongtao
 */
public class NameNodeInfo {
    public static final String PATH = "/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus";
    public static final String NAMENODE1_URL = "http://" + PropertiesInfo.getConfProperty().getProperty("NameNode1");
    public static final String NAMENODE2_URL = "http://" + PropertiesInfo.getConfProperty().getProperty("NameNode2");

    public static String getActiveMasterUrl() {
        String flag1 = ConnectInfo.connectWithoutAUTH(NAMENODE1_URL, PATH).getJSONArray("beans").getJSONObject(0).getString("State");
        String url = "active".equals(flag1) ? NAMENODE1_URL : NAMENODE2_URL;
        return url;
    }

    public static ArrayList<String> getHDFSInfo(String baseUrl) {
        ArrayList<String> hdfsInfo = new ArrayList();

        JSONObject nameNodeInfo = ConnectInfo.connectWithoutAUTH(baseUrl, "/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo");
        JSONObject fsNameSystemStats = ConnectInfo.connectWithoutAUTH(baseUrl, "/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState");
        JSONObject nameNodeMemory = ConnectInfo.connectWithoutAUTH(baseUrl, "/jmx?qry=java.lang:type=Memory");

        JSONObject jsonObject1 = nameNodeInfo.getJSONArray("beans").getJSONObject(0);
        JSONObject jsonObject2 = fsNameSystemStats.getJSONArray("beans").getJSONObject(0);
        JSONObject jsonObject3 = nameNodeMemory.getJSONArray("beans").getJSONObject(0);



        hdfsInfo.add("0");
        hdfsInfo.add(jsonObject1.getString("Version").split(",")[0]);
        hdfsInfo.add(jsonObject1.getString("NonDfsUsedSpace"));
        hdfsInfo.add(jsonObject1.getString("BlockPoolUsedSpace"));
        hdfsInfo.add(jsonObject2.getString("CapacityTotal"));
        hdfsInfo.add(jsonObject2.getString("CapacityUsed"));
        hdfsInfo.add(jsonObject2.getString("CapacityRemaining"));
        hdfsInfo.add(jsonObject2.getString("NumLiveDataNodes"));
        hdfsInfo.add(jsonObject2.getString("NumDeadDataNodes"));
        hdfsInfo.add(jsonObject2.getString("NumDecommissioningDataNodes"));
        int numInMaintenanceDataNodes = jsonObject2.getIntValue("NumInMaintenanceLiveDataNodes") + jsonObject2.getIntValue("NumInMaintenanceDeadDataNodes") + jsonObject2.getIntValue("NumEnteringMaintenanceDataNodes");
        hdfsInfo.add(String.valueOf(numInMaintenanceDataNodes));
        hdfsInfo.add(jsonObject2.getString("UnderReplicatedBlocks"));
        hdfsInfo.add(jsonObject3.getJSONObject("HeapMemoryUsage").getString("used"));
//        hdfsInfo.add(jsonObject3.getJSONObject("HeapMemoryUsage").getString("max"));
        hdfsInfo.add(jsonObject3.getJSONObject("NonHeapMemoryUsage").getString("used"));
//        hdfsInfo.add(jsonObject3.getJSONObject("NonHeapMemoryUsage").getString("committed"));
        hdfsInfo.add(jsonObject2.getString("BlocksTotal").trim());
        hdfsInfo.add(jsonObject2.getString("FilesTotal").trim());
        return hdfsInfo;
    }
}
