package com.nr.testapp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
    private static final Logger log = LoggerFactory.getLogger(App.class);
     public static void main(String[] args) {
         log.info("begin app");

         while (!Thread.currentThread().isInterrupted()) {
             log.info("--------begin job--------");
             KinesisSync.runKinesisSync();
             KinesisAsync.runKinesisAsync();
             KinesisV1Async.runKinesisAsync();
             log.info("--------end job--------");
             try {
                 Thread.sleep(2000);
             } catch (InterruptedException e) {
                 log.error(e.getMessage(), e);
             }
         }
         log.info("end app");
     }
}
