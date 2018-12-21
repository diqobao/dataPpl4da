package com.datappl;

import com.datappl.SparkSink.SparkConnector;
import com.datappl.SparkSink.SparkConnector2;

public class SparkTest {
    public static void main(String[] args) {
        SparkConnector2.createNewSparkServer("1", new String[]{"US"});
    }
}
