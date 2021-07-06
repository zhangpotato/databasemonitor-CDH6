package com.longdb.hbase;

import com.longdb.connect.PropertiesInfo;
import org.jsoup.Jsoup;

import java.io.IOException;

/**
 * @author hongtao
 */
public class HMasterInfo {

    public static final String HMASTER1_URL = "http://" + PropertiesInfo.getConfProperty().getProperty("HMaster1") + "/master-status";
    public static final String HMASTER2_URL = "http://" + PropertiesInfo.getConfProperty().getProperty("HMaster2") + "/master-status";

    public static String getActiveMasterUrl() {
        String flag1 = null;
        try {
            flag1 = Jsoup.connect(HMASTER1_URL).get().title().split(":")[0];
        } catch (IOException e) {
            e.printStackTrace();
        }
        String url = "Master".equals(flag1) ? HMASTER1_URL : HMASTER2_URL;

        return url;
    }
}
