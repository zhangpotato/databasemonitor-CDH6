package com.longdb;

import com.longdb.prometheus.*;

/**
 * @author hongtao
 */
public class Tool {
    public static void main(String[] args) {
        HBaseThread hbaseThread = new HBaseThread();
        HDFSThread hdfsThread = new HDFSThread();
        LongDBThread longDBThread = new LongDBThread();
        YarnThread yarnThread = new YarnThread();
        ServiceThread serviceThread = new ServiceThread();

        Thread hbase = new Thread(hbaseThread,"HBase");
        Thread hdfs = new Thread(hdfsThread,"HDFS");
        Thread longdb = new Thread(longDBThread,"LongDB");
        Thread yarn = new Thread(yarnThread,"Yarn");
        Thread service = new Thread(serviceThread,"Service");

        hbase.start();
        hdfs.start();
        longdb.start();
        yarn.start();
        service.start();
    }
}
