package com.nr.testapp;

public class Config {
    public static final String STREAM_NAME = "java-test-stream"; // java-test-stream
    public static final String REGION = "us-east-2"; // us-east-2
    public static final software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider V2_CREDENTIALS_PROVIDER =
            software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider.create("default");
    public static final com.amazonaws.auth.profile.ProfileCredentialsProvider V1_CREDENTIALS_PROVIDER =
            new com.amazonaws.auth.profile.ProfileCredentialsProvider("default");

    }
