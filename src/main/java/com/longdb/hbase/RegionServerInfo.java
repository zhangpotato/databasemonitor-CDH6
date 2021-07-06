package com.longdb.hbase;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Pattern;

/**
 * @author hongtao
 */
public class RegionServerInfo {

    public static HashMap<String, ArrayList> getRegionServerInfo(String url) {
        Document doc = null;
        Elements baseStats = null;
        Elements memory = null;
        Elements requests = null;
        Elements storefiles = null;
        Elements compactions = null;
        String regEx="[^0-9]";
        Pattern p = Pattern.compile(regEx);
        HashMap<String, ArrayList> regionServersInfo = new HashMap();

        try {
            doc = Jsoup.connect(url).get();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (doc != null) {
            baseStats = doc.getElementById("tab_baseStats").getElementsByTag("tr");
            memory = doc.getElementById("tab_memoryStats").getElementsByTag("tr");
            requests = doc.getElementById("tab_requestStats").getElementsByTag("tr");
            storefiles = doc.getElementById("tab_storeStats").getElementsByTag("tr");
            compactions = doc.getElementById("tab_compactStats").getElementsByTag("tr");
        }

        for (int i = 1; i < baseStats.size() - 1; i++) {

            ArrayList regionServerInfo = new ArrayList();

            for (int j = 1; j < baseStats.get(i).childrenSize(); j++) {
                if(j == 2){
                    regionServerInfo.add(baseStats.get(i).child(j).text().split(" ")[0].trim());
                }else {
                    regionServerInfo.add(baseStats.get(i).child(j).text());
                }
            }

            for (int j = 1; j < memory.get(i).childrenSize(); j++) {
                if(memory.get(i).child(j).text().contains("GB")){
                    regionServerInfo.add(String.valueOf(Double.parseDouble(memory.get(i).child(j).text().split(" ")[0].trim())*1024));
                }else {
                    regionServerInfo.add(memory.get(i).child(j).text().split(" ")[0].trim());
                }
            }

            for (int j = 1; j < requests.get(i).childrenSize(); j++) {
                regionServerInfo.add(p.matcher(requests.get(i).child(j).text()).replaceAll("").trim());
            }

            for (int j = 1; j < storefiles.get(i).childrenSize(); j++) {
                regionServerInfo.add(p.matcher(storefiles.get(i).child(j).text()).replaceAll("").trim());
            }

            for (int j = 1; j < compactions.get(i).childrenSize() - 1; j++) {
                regionServerInfo.add(p.matcher(compactions.get(i).child(j).text()).replaceAll("").trim());
            }
            regionServersInfo.put(baseStats.get(i).child(0).text().split(",")[0], regionServerInfo);
        }
        return regionServersInfo;
    }
}
