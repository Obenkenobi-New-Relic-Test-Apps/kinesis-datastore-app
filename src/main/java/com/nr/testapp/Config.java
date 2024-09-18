package com.nr.testapp;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;

public class Config {
    public static final String STREAM_NAME = "java-test-stream"; // java-test-stream
    public static final String REGION = "us-east-2"; // us-east-2
    public static final ProfileCredentialsProvider CREDENTIALS_PROVIDER = ProfileCredentialsProvider.create("default");

    }
