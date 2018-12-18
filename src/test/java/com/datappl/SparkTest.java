package com.datappl;

import com.datappl.SparkSink.SparkConnector;

public class SparkTest {
    public static void main(String[] args) {
        SparkConnector.createNewSparkServer("1", new String[]{"test"});
    }
}
