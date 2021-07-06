package com.longdb.connect;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author hongtao
 */
public class PropertiesInfo {
    public static final String CONF_PATH = "/conf/monitor.properties";

    public static Properties getConfProperty()  {
        String filePath = System.getProperty("user.dir");
        Properties properties = new Properties();
        InputStream in = null;

        try {
            in = new FileInputStream(filePath + CONF_PATH);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        try {
            properties.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }
}
