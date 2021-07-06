package com.longdb.cm;

import com.cloudera.api.ClouderaManagerClientBuilder;
import com.cloudera.api.DataView;
import com.cloudera.api.model.ApiCluster;
import com.cloudera.api.model.ApiClusterList;
import com.cloudera.api.model.ApiRole;
import com.cloudera.api.v11.RolesResourceV11;
import com.cloudera.api.v18.ClustersResourceV18;
import com.cloudera.api.v18.ServicesResourceV18;
import com.cloudera.api.v19.RootResourceV19;
import com.longdb.connect.PropertiesInfo;

import java.io.IOException;
import java.util.HashMap;

/**
 * @author hongtao
 */
public class HBaseStatus {
    public static final String CM_HOST = PropertiesInfo.getConfProperty().getProperty("cmHost");
    public static final int CM_PORT = Integer.valueOf(PropertiesInfo.getConfProperty().getProperty("cmPort"));
    public static final String CM_USERNAME = PropertiesInfo.getConfProperty().getProperty("cmUsername");
    public static final String CM_PASSWORD = PropertiesInfo.getConfProperty().getProperty("cmPassword");

    public static HashMap getServiceStatus() {
        HashMap<String, String> maxNode = new HashMap<>();
        HashMap<String, String> regionServerNumber = new HashMap<>();
        HashMap<String, String> masterAliveNumber = new HashMap<>();
        HashMap<String, String> masterDeadNumber = new HashMap<>();
        HashMap<String, Integer> serviceNumber = new HashMap<>();

        RootResourceV19 apiRoot = new ClouderaManagerClientBuilder().withHost(CM_HOST).withPort(CM_PORT)
                .withUsernamePassword(CM_USERNAME, CM_PASSWORD).build().getRootV19();

        ClustersResourceV18 clustersResourceV18 = apiRoot.getClustersResource();
        ApiClusterList apiClusters = clustersResourceV18.readClusters(DataView.SUMMARY);
        for (ApiCluster apiCluster1 : apiClusters) {
            ServicesResourceV18 resource = clustersResourceV18.getServicesResource(apiCluster1.getName());
            RolesResourceV11 rolesResourceV11 = resource.getRolesResource("hbase");
            for (ApiRole apiRole : rolesResourceV11.readRoles()) {
                if ("REGIONSERVER".equals(apiRole.getType())) {
                    regionServerNumber.put(apiRole.getHostRef().getHostId(), apiRole.getEntityStatus().toString());
                } else if ("MASTER".equals(apiRole.getType())) {
                    if ("GOOD_HEALTH".equals(apiRole.getEntityStatus().toString())) {
                        masterAliveNumber.put(apiRole.getHostRef().getHostId(), apiRole.getEntityStatus().toString());
                    } else {
                        masterDeadNumber.put(apiRole.getHostRef().getHostId(), apiRole.getEntityStatus().toString());
                    }

                } else {
                    continue;
                }
            }
        }
        //获取hdfs副本数
        try {
            String replication = HDFSStatus.getHDFSReplication(CM_HOST, CM_PORT, CM_USERNAME, CM_PASSWORD);
            if (replication == null || replication.length() == 0) {
                serviceNumber.put("replication", 0);
            } else {
                serviceNumber.put("replication", Integer.parseInt(replication));
            }
        } catch (IOException e) {
            serviceNumber.put("replication", 0);
            e.printStackTrace();
        }

        maxNode.putAll(regionServerNumber);
        maxNode.putAll(masterAliveNumber);
        maxNode.putAll(masterDeadNumber);

        serviceNumber.put("maxNode", maxNode.size());
        serviceNumber.put("regionServerNumber", regionServerNumber.size());
        serviceNumber.put("masterAliveNumber", masterAliveNumber.size());
        serviceNumber.put("masterDeadNumber", masterDeadNumber.size());


        return serviceNumber;
    }

}
