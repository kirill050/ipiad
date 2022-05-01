package com.company;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class Main {

    public static void main(String[] args) {
        // write your code here
        Config conf = ConfigFactory.load();
        ElasticConnector esCon = new ElasticConnector();
        esCon.initialize(conf.getConfig("es"));

        esCon.getSomeDataAll();
        esCon.getSomeData();
    }
}
