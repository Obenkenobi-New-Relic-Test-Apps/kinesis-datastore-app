package com.nr.testapp;

public class App {
     public static void main(String[] args) {
         KinesisSync.runKinesisSync();
         KinesisAsync.runKinesisAsync();
     }
}
