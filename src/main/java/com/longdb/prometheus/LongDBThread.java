package com.longdb.prometheus;

import java.io.IOException;

/**
 * @author hongtao
 */
public class LongDBThread implements Runnable {
    @Override
    public void run() {
        while (true) {
            try {
                PushMetrics.sendLongDBMetrics();
            } catch (IOException e) {
                e.printStackTrace();
                try {
                    PushMetrics.sendLongDBMetrics();
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                }
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
