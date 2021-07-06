package com.longdb.prometheus;

import java.io.IOException;

/**
 * @author hongtao
 */
public class YarnThread implements Runnable {
    @Override
    public void run() {
        while (true) {
            try {
                PushMetrics.sendResourceManagerMetrics();
            } catch (IOException e) {
                e.printStackTrace();
                try {
                    PushMetrics.sendResourceManagerMetrics();
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                }
            }

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
