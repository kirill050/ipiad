package com.company;

import com.typesafe.config.Config;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ElasticConnector {
    private Config config;
    private PreBuiltTransportClient client;

    private PreBuiltTransportClient createClient() throws UnknownHostException {
        Settings settings = Settings.builder()
                .put("cluster.name", config.getString("cluster"))
                .build();

        PreBuiltTransportClient cli = new PreBuiltTransportClient(settings);
        cli.addTransportAddress(
                new TransportAddress(InetAddress.getByName(config.getString("host")), 9300)
        );
        return cli;
    }

    //Fixme: unsafe! check for existing client or close app!
    void initialize(Config conf) {
        config = conf;
        try {
            client = createClient();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    void getSomeDataAll() {
        QueryBuilder query = QueryBuilders.matchAllQuery();
        SearchResponse response = client.prepareSearch("*").setQuery(query).get();
        System.out.println(response.getHits().getTotalHits());
    }


    void getSomeData() {
        QueryBuilder query = QueryBuilders.termQuery("nickname", "vasya");
        SearchResponse response = client.prepareSearch("indice-name").setQuery(query).get();
        System.out.println(response.getHits().getTotalHits());
    }

    void getSomeDataList() {
        QueryBuilder query = QueryBuilders.termQuery("nickname", "vasya");
        SearchResponse response = client.prepareSearch("indice-name").setQuery(query).get();

        Iterator<SearchHit> sHits = response.getHits().iterator();
        List<String> results = new ArrayList<String>(20); //some hack! initial size of array!
        while (sHits.hasNext()) {
            results.add(sHits.next().getSourceAsString());
        //jackson

        }

        System.out.println(response.getHits().getTotalHits());
    }
}
