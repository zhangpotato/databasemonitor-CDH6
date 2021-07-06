package com.longdb.prometheus;

import com.longdb.cm.HBaseStatus;
import com.longdb.cm.ServiceStatus;
import com.longdb.connect.PropertiesInfo;
import com.longdb.haproxy.ClusterStatus;
import com.longdb.hbase.HMasterInfo;
import com.longdb.hbase.RegionServerInfo;
import com.longdb.hdfs.NameNodeInfo;
import com.longdb.yarn.ResourceManagerInfo;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author hongtao
 */
public class PushMetrics {
    public static final String PUSHGATEWAY_URL = PropertiesInfo.getConfProperty().getProperty("pushGateWayUrl");

    public static void sendLongDBMetrics() throws IOException {
        PushGateway pg = new PushGateway(PUSHGATEWAY_URL);
        Map<String, String> groupingKey = new HashMap<String, String>();

        HashMap<String, Integer> serviceNumber = HBaseStatus.getServiceStatus();
        HashMap<String, Integer> clusterStatus = ClusterStatus.getCurrentClusterStatus();

        CollectorRegistry registry = new CollectorRegistry();

        Gauge guage = Gauge.build("longdb_cluster_info", "数据库信息收集").labelNames("type").create();
        String datetime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        guage.labels("longdb-agent-max-count").set(serviceNumber.get("regionServerNumber"));
        guage.labels("longdb-agent-current-count").set(clusterStatus.get("currentRegionServerCount"));
        guage.labels("longdb-server-alive-count").set(serviceNumber.get("masterAliveNumber"));
        guage.labels("longdb-sessions-count").set(clusterStatus.get("currentSessionsCount"));
        guage.labels("longdb-replication-number").set(serviceNumber.get("replication"));
        guage.labels("longdb-server-count").set((serviceNumber.get("masterAliveNumber") + serviceNumber.get("masterDeadNumber")));

        guage.register(registry);

        groupingKey.put("instance", "my_instance");
        pg.pushAdd(registry, "pushgateway", groupingKey);
        System.out.println("LongDB指标：" + datetime + "信息发送成功！" +
                "最大Agent数：" + serviceNumber.get("regionServerNumber") + "当前Agent存活数：" + clusterStatus.get("currentRegionServerCount") + "当前Server存活数：" +
                serviceNumber.get("masterAliveNumber") + "当前连接数：" + clusterStatus.get("currentSessionsCount") + "副本数：" +
                serviceNumber.get("replication") + "Master个数：" + (serviceNumber.get("masterAliveNumber") + serviceNumber.get("masterDeadNumber")));

    }

    public static void sendHDFSMetrics() throws IOException {
        PushGateway pg = new PushGateway(PUSHGATEWAY_URL);
        Map<String, String> groupingKey = new HashMap<String, String>();
        CollectorRegistry registry = new CollectorRegistry();

        String baseUrl = NameNodeInfo.getActiveMasterUrl();
        ArrayList<String> hdfsInfo = NameNodeInfo.getHDFSInfo(baseUrl);

        Gauge guage = Gauge.build("longdb_cluster_hdfs_info", "集群信息收集").labelNames("type", "version").create();
        String datetime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        guage.labels("longdb-hdfs-NNUpTime", hdfsInfo.get(1)).set(Double.parseDouble(hdfsInfo.get(0)));
        guage.labels("longdb-hdfs-NonDfsUsedSpace", hdfsInfo.get(1)).set(Double.parseDouble(hdfsInfo.get(2)));
        guage.labels("longdb-hdfs-BlockPoolUsedSpace", hdfsInfo.get(1)).set(Double.parseDouble(hdfsInfo.get(3)));
        guage.labels("longdb-hdfs-CapacityTotal", hdfsInfo.get(1)).set(Double.parseDouble(hdfsInfo.get(4)));
        guage.labels("longdb-hdfs-CapacityUsed", hdfsInfo.get(1)).set(Double.parseDouble(hdfsInfo.get(5)));
        guage.labels("longdb-hdfs-CapacityRemaining", hdfsInfo.get(1)).set(Double.parseDouble(hdfsInfo.get(6)));
        guage.labels("longdb-hdfs-NumLiveDataNodes", hdfsInfo.get(1)).set(Double.parseDouble(hdfsInfo.get(7)));
        guage.labels("longdb-hdfs-NumDeadDataNodes", hdfsInfo.get(1)).set(Double.parseDouble(hdfsInfo.get(8)));
        guage.labels("longdb-hdfs-NumDecommissioningDataNodes", hdfsInfo.get(1)).set(Double.parseDouble(hdfsInfo.get(9)));
        guage.labels("longdb-hdfs-NumInMaintenanceDataNodes", hdfsInfo.get(1)).set(Double.parseDouble(hdfsInfo.get(10)));
        guage.labels("longdb-hdfs-UnderReplicatedBlocks", hdfsInfo.get(1)).set(Double.parseDouble(hdfsInfo.get(11)));
        guage.labels("longdb-hdfs-HeapMemoryUsage", hdfsInfo.get(1)).set(Double.parseDouble(hdfsInfo.get(12)));
        guage.labels("longdb-hdfs-NonHeapMemoryUsage", hdfsInfo.get(1)).set(Double.parseDouble(hdfsInfo.get(13)));
        guage.labels("longdb-hdfs-BlocksTotal", hdfsInfo.get(1)).set(Double.parseDouble(hdfsInfo.get(14)));
        guage.labels("longdb-hdfs-FilesTotal", hdfsInfo.get(1)).set(Double.parseDouble(hdfsInfo.get(15)));


        guage.register(registry);
        groupingKey.put("instance", "my_instance");
        pg.pushAdd(registry, "pushgateway", groupingKey);
        System.out.println("HDFS指标：" + datetime + hdfsInfo);
    }

    public static void sendHBaseMetrics() throws IOException {
        PushGateway pg = new PushGateway(PUSHGATEWAY_URL);
        Map<String, String> groupingKey = new HashMap<String, String>();
        CollectorRegistry registry = new CollectorRegistry();

        String baseUrl = HMasterInfo.getActiveMasterUrl();
        HashMap<String, ArrayList> regionServersInfo = RegionServerInfo.getRegionServerInfo(baseUrl);

        Gauge guage = Gauge.build("longdb_cluster_hbase_info", "集群信息收集").labelNames("type", "version", "host").create();
        String datetime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        for (Map.Entry<String, ArrayList> entry : regionServersInfo.entrySet()) {
            String version = entry.getValue().get(2).toString();
            guage.labels("longdb-hbase-RequestsPerSecond", version, entry.getKey()).set(Double.parseDouble(entry.getValue().get(3).toString()));
            guage.labels("longdb-hbase-NumRegions", version, entry.getKey()).set(Double.parseDouble(entry.getValue().get(4).toString()));
            guage.labels("longdb-hbase-UsedHeap", version, entry.getKey()).set(Double.parseDouble(entry.getValue().get(5).toString()));
            guage.labels("longdb-hbase-MaxHeap", version, entry.getKey()).set(Double.parseDouble(entry.getValue().get(6).toString()));
            guage.labels("longdb-hbase-MemstoreSize", version, entry.getKey()).set(Double.parseDouble(entry.getValue().get(7).toString()));
            guage.labels("longdb-hbase-RequestPerSecond", version, entry.getKey()).set(Double.parseDouble(entry.getValue().get(8).toString()));
            guage.labels("longdb-hbase-ReadRequestCount", version, entry.getKey()).set(Double.parseDouble(entry.getValue().get(9).toString()));
            guage.labels("longdb-hbase-WriteRequestCount", version, entry.getKey()).set(Double.parseDouble(entry.getValue().get(11).toString()));
            guage.labels("longdb-hbase-NumStores", version, entry.getKey()).set(Double.parseDouble(entry.getValue().get(12).toString()));
            guage.labels("longdb-hbase-NumStorefiles", version, entry.getKey()).set(Double.parseDouble(entry.getValue().get(13).toString()));
            guage.labels("longdb-hbase-StorefileSizeUncompressed", version, entry.getKey()).set(Double.parseDouble(entry.getValue().get(14).toString()));
            guage.labels("longdb-hbase-StorefileSize", version, entry.getKey()).set(Double.parseDouble(entry.getValue().get(15).toString()));
            guage.labels("longdb-hbase-IndexSize", version, entry.getKey()).set(Double.parseDouble(entry.getValue().get(16).toString()));
            guage.labels("longdb-hbase-BloomSize", version, entry.getKey()).set(Double.parseDouble(entry.getValue().get(17).toString()));
            guage.labels("longdb-hbase-NumCompactingKVs", version, entry.getKey()).set(Double.parseDouble(entry.getValue().get(18).toString()));
            guage.labels("longdb-hbase-NumCompactedKVs", version, entry.getKey()).set(Double.parseDouble(entry.getValue().get(19).toString()));
            guage.labels("longdb-hbase-RemainingKVs", version, entry.getKey()).set(Double.parseDouble(entry.getValue().get(20).toString()));
        }
        guage.register(registry);
        groupingKey.put("instance", "my_instance");
        pg.pushAdd(registry, "pushgateway", groupingKey);
        System.out.println("HBase指标：" + datetime + regionServersInfo);
    }

    public static void sendResourceManagerMetrics() throws IOException {
        PushGateway pg = new PushGateway(PUSHGATEWAY_URL);
        Map<String, String> groupingKey = new HashMap<String, String>();
        CollectorRegistry registry = new CollectorRegistry();
        String url = ResourceManagerInfo.getActiveResourceManagerUrl();
        ArrayList<String> resourceManagerInfo = ResourceManagerInfo.getResourceManagerInfo(url);

        Gauge guage = Gauge.build("longdb_cluster_yarn_info", "集群信息收集").labelNames("type").create();
        String datetime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        guage.labels("longdb-yarn-AppsSubmitted").set(Double.parseDouble(resourceManagerInfo.get(0)));
        guage.labels("longdb-yarn-AppsPending").set(Double.parseDouble(resourceManagerInfo.get(1)));
        guage.labels("longdb-yarn-AppsRunning").set(Double.parseDouble(resourceManagerInfo.get(2)));
        guage.labels("longdb-yarn-AppsCompleted").set(Double.parseDouble(resourceManagerInfo.get(3)));
        guage.labels("longdb-yarn-ContainersRunning").set(Double.parseDouble(resourceManagerInfo.get(4)));
        guage.labels("longdb-yarn-MemoryUsed").set(Double.parseDouble(resourceManagerInfo.get(5)));
        guage.labels("longdb-yarn-MemoryTotal").set(Double.parseDouble(resourceManagerInfo.get(6)));
        guage.labels("longdb-yarn-MemoryReserved").set(Double.parseDouble(resourceManagerInfo.get(7)));
        guage.labels("longdb-yarn-VCoresUsed").set(Double.parseDouble(resourceManagerInfo.get(8)));
        guage.labels("longdb-yarn-VCoresTotal").set(Double.parseDouble(resourceManagerInfo.get(9)));
        guage.labels("longdb-yarn-VCoresReserved").set(Double.parseDouble(resourceManagerInfo.get(10)));
        guage.labels("longdb-yarn-ActiveNodes").set(Double.parseDouble(resourceManagerInfo.get(11)));
        guage.labels("longdb-yarn-DecommissioningNodes").set(Double.parseDouble(resourceManagerInfo.get(12)));
        guage.labels("longdb-yarn-DecommissionedNodes").set(Double.parseDouble(resourceManagerInfo.get(13)));
        guage.labels("longdb-yarn-LostNodes").set(Double.parseDouble(resourceManagerInfo.get(14)));
        guage.labels("longdb-yarn-UnhealthyNodes").set(Double.parseDouble(resourceManagerInfo.get(15)));
        guage.labels("longdb-yarn-RebootedNodes").set(Double.parseDouble(resourceManagerInfo.get(16)));
        guage.labels("longdb-yarn-UserAppsSubmitted").set(Double.parseDouble(resourceManagerInfo.get(17)));
        guage.labels("longdb-yarn-UserAppsPending").set(Double.parseDouble(resourceManagerInfo.get(18)));
        guage.labels("longdb-yarn-UserAppsRunning").set(Double.parseDouble(resourceManagerInfo.get(19)));
        guage.labels("longdb-yarn-UserAppsCompleted").set(Double.parseDouble(resourceManagerInfo.get(20)));
        guage.labels("longdb-yarn-UserContainersRunning").set(Double.parseDouble(resourceManagerInfo.get(21)));
        guage.labels("longdb-yarn-UserContainersPending").set(Double.parseDouble(resourceManagerInfo.get(22)));
        guage.labels("longdb-yarn-UserContainersReserved").set(Double.parseDouble(resourceManagerInfo.get(23)));
        guage.labels("longdb-yarn-UserMemoryUsed").set(Double.parseDouble(resourceManagerInfo.get(24)));
        guage.labels("longdb-yarn-UserMemoryPending").set(Double.parseDouble(resourceManagerInfo.get(25)));
        guage.labels("longdb-yarn-UserMemoryReserved").set(Double.parseDouble(resourceManagerInfo.get(26)));
        guage.labels("longdb-yarn-UserVCoresUsed").set(Double.parseDouble(resourceManagerInfo.get(27)));
        guage.labels("longdb-yarn-UserVCoresPending").set(Double.parseDouble(resourceManagerInfo.get(28)));
        guage.labels("longdb-yarn-UserVCoresReservedd").set(Double.parseDouble(resourceManagerInfo.get(29)));

        guage.register(registry);
        groupingKey.put("instance", "my_instance");
        pg.pushAdd(registry, "pushgateway", groupingKey);

        System.out.println("Yarn指标：" + datetime + resourceManagerInfo);
        resourceManagerInfo.clear();
    }

    public static void sendServiceStatus() throws IOException {
        PushGateway pg = new PushGateway(PUSHGATEWAY_URL);
        Map<String, String> groupingKey = new HashMap<String, String>();
        HashMap<String, Integer> serviceStatus = ServiceStatus.getServiceStatus();

        CollectorRegistry registry = new CollectorRegistry();

        Gauge guage = Gauge.build("longdb_service_info", "服务信息收集").labelNames("service","type").create();
        String datetime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        for (Map.Entry<String, Integer> entry : serviceStatus.entrySet()) {
//            guage.labels(entry.getKey(),String.format("longdb-service-%s", entry.getKey())).set(entry.getValue());
            guage.labels(entry.getKey(),"longdb-service").set(entry.getValue());
        }

        guage.register(registry);

        groupingKey.put("instance", "my_instance");
        pg.pushAdd(registry, "pushgateway", groupingKey);
        System.out.println("状态指标：" + datetime + serviceStatus);
    }

}
