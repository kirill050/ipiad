import com.alibaba.fastjson.JSON;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.codelibs.minhash.MinHash;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.filter.Filters;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import static org.elasticsearch.common.xcontent.XContentFactory.*;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.*;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;

import org.apache.lucene.analysis.core.WhitespaceTokenizer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class Elastic_talker  extends Thread  {
    private Config config;
    private PreBuiltTransportClient client;

    String Queue_name = "elastic_Queue";
    String exchangeName = "Exchange_btw_threads";
    Channel channel;
    private String message;
    private long tag;

    // The number of bits for each hash value.
    int hashBit = 1;
    // A base seed for hash functions.
    int seed = 0;
    // The number of hash functions.
    int num = 128;

    QueueingConsumer consumer;
    String routingKey_elastic = "Route_to_elastic";
    private static Logger log = LogManager.getLogger();

    int thread_type = 0; // 0 -- main elastic talker thread
                         // 1 -- little elastic talker thread (just for answering rabbit messages)

    int threads_number = 5;
    boolean durable;
    Map<String, Object> map_args;


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

    public Elastic_talker(int thread_type){
        this.thread_type = thread_type;
    }
    public Elastic_talker(int thread_type, PreBuiltTransportClient client, Channel channel, String message,
                          long tag) {
        this.thread_type = thread_type;
        this.client = client;
        this.channel = channel;
        this.message = message;
        this.tag = tag;
    }
    XContentBuilder get_mapping() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder();
        mapping.startObject();
        {
            mapping.startObject("properties");
            {
                mapping.startObject("url");
                {
                    mapping.field("type", "text");
                }
                mapping.endObject();
                mapping.startObject("topic");
                {
                    mapping.field("type", "text");
                }
                mapping.endObject();
                mapping.startObject("date");
                {
                    mapping.field("type", "text");
                }
                mapping.endObject();
                mapping.startObject("author");
                {
                    mapping.field("type", "text");
                }
                mapping.endObject();
                mapping.startObject("filling");
                {
                    mapping.field("type", "text");
                    mapping.field("analyzer", "minhash_analyzer");
                }
                mapping.endObject();
                mapping.startObject("minhash_value");
                {
                    mapping.field("type", "text");
                }
                mapping.endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();
        return mapping;
    }
    XContentBuilder get_analyzer_settings() throws IOException {
        XContentBuilder analyzer_settings = XContentFactory.jsonBuilder();
        analyzer_settings.startObject();
        {
            analyzer_settings.startObject("analysis");
            {
                analyzer_settings.startObject("filter");
                {
                    analyzer_settings.startObject("shingle_filter");
                    {
                        analyzer_settings.field("type", "shingle");
                        analyzer_settings.field("min_shingle_size", 5);
                        analyzer_settings.field("max_shingle_size", 5);
                        analyzer_settings.field("output_unigrams", false);
                    }
                    analyzer_settings.endObject();
                    analyzer_settings.startObject("minhash_filter");
                    {
                        analyzer_settings.field("type", "min_hash");
                        analyzer_settings.field("hash_count", 1);
                        analyzer_settings.field("bucket_count", 512);
                        analyzer_settings.field("hash_set_size", 1);
                        analyzer_settings.field("with_rotation", true);
                    }
                    analyzer_settings.endObject();
                    analyzer_settings.startObject("easy_rus_stemmer");
                    {
                        analyzer_settings.field("type", "stemmer");
                        analyzer_settings.field("language", "russian");
                    }
                    analyzer_settings.endObject();
                }
                analyzer_settings.endObject();

                analyzer_settings.startObject("analyzer");
                {
                    analyzer_settings.startObject("minhash_analyzer");
                    {
                        analyzer_settings.field("tokenizer", "standard");
                        analyzer_settings.field("filter", new String[]{"shingle_filter", "minhash_filter"});
                    }
                    analyzer_settings.endObject();
                }
                analyzer_settings.endObject();
            }
            analyzer_settings.endObject();
        }
        analyzer_settings.endObject();
        return analyzer_settings;
    }
    void initialize(Config conf) throws IOException, TimeoutException, ExecutionException, InterruptedException {
        config = conf;
        try {
            client = createClient();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }


        CreateIndexRequest indexRequest = new CreateIndexRequest("site_logs");
        indexRequest.settings(Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 1)
                .build()
        );

        indexRequest.settings(get_analyzer_settings());

        try {
            client.admin().indices().create(indexRequest).actionGet();
        } catch (ResourceAlreadyExistsException e){}

        PutMappingRequest putMappingRequest = new PutMappingRequest("site_logs");
        XContentBuilder builder = get_mapping();
        putMappingRequest.type("page")
                .source(builder);
        client.admin().indices().putMapping(putMappingRequest).get();

        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("rabbitmq");
        factory.setPassword("rabbitmq");
        factory.setVirtualHost("/");
        factory.setHost("127.0.0.1");
        factory.setPort(5672);
        Connection conn = factory.newConnection();
        channel = conn.createChannel();
        durable = true;
        map_args = new HashMap<String, Object>();
        channel.exchangeDeclare(exchangeName, "direct", durable);
        map_args.put("x-max-length", 100);
        channel.queueDeclare(Queue_name, durable, false, false, map_args);
        channel.queueBind(Queue_name, exchangeName, routingKey_elastic);
        channel.queuePurge(Queue_name);
        consumer = new QueueingConsumer(channel);
        channel.basicConsume(Queue_name, false, consumer);
    }

    void pushSomeData(info_for_elastic message) throws NoSuchAlgorithmException, IOException {
        MessageDigest md = MessageDigest.getInstance("MD5"); //getting md5 algorithm for _id

        Map<String, Object> mapa = new HashMap<String, Object>();
        mapa.put("url", message.url);
        mapa.put("topic", message.topic);
        mapa.put("date", message.date);
        mapa.put("author", message.author);
        mapa.put("filling", message.filling);
        mapa.put("minhash_value", MinHash.calculate(MinHash.createAnalyzer(hashBit, seed, num), message.filling));

IndexResponse response = client.prepareIndex("site_logs", "page", md.digest(mapa.toString().getBytes()).toString())
                .setSource(mapa, XContentType.JSON)
                .get();
    }

    void find_dublicates(){
        //TODO: make aggregation query to find articles with minhash that is similar in diapazon ukazanniy nizhe

        QueryBuilder query = QueryBuilders.matchAllQuery();
        SearchResponse response = client.prepareSearch("site_logs").setQuery(query).setSize(1000).get();

        Iterator<SearchHit> sHits = response.getHits().iterator();
        List<String> minhashes = new ArrayList<String>(20);
        List<String> doc_topic = new ArrayList<String>(20);
        while (sHits.hasNext()) {
            minhashes.add(sHits.next().getSourceAsMap().get("minhash_value").toString());
            doc_topic.add(sHits.next().getSourceAsMap().get("topic").toString());
        }
        for (int i = 0; i < minhashes.size(); ++i){
            System.out.println("Similar to" + doc_topic.get(i));
            for (int j = 0; j < minhashes.size(); ++j) {
                if (i == j) continue;
                if (MinHash.compare(minhashes.get(i).getBytes(), minhashes.get(j).getBytes()) > 0.9){
                    System.out.println("\t" + doc_topic.get(j));
                }
            }
        }
    }

    void getSomeDataAll() {
        QueryBuilder query = QueryBuilders.matchAllQuery();
        SearchResponse response = client.prepareSearch("site_logs").setQuery(query).setSize(1000).get();

        Iterator<SearchHit> sHits = response.getHits().iterator();
        List<String> results = new ArrayList<String>(20);
        while (sHits.hasNext()) {
            results.add(sHits.next().getSourceAsString());
        }
        for (String it : results){
            System.out.println(it);
        }
        log.info(response.getHits().getTotalHits());
    }
    void getSomeDataList(String field_name, String value) {
        QueryBuilder query = QueryBuilders.matchQuery(field_name, value);
        SearchResponse response = client.prepareSearch("site_logs").setQuery(query).get();

        Iterator<SearchHit> sHits = response.getHits().iterator();
        List<String> results = new ArrayList<String>(20);
        while (sHits.hasNext()) {
            results.add(sHits.next().getSourceAsString());
        }
        for (String it : results){
            System.out.println(it);
        }

        System.out.println(response.getHits().getTotalHits());
    }
    void getCountAggregation(String field_name) {

        String for_query = field_name + ".keyword";
        QueryBuilder query = QueryBuilders.matchAllQuery();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        TermsAggregationBuilder aggregation = AggregationBuilders.terms("field_count")
                .field(for_query);
        searchSourceBuilder.aggregation(aggregation);
        SearchResponse searchResponse = client.prepareSearch("site_logs").setQuery(query).addAggregation(aggregation).get();
        Terms terms = searchResponse.getAggregations().get("field_count");
        //Get results
        for (Terms.Bucket bucket : terms.getBuckets()) {
            System.out.println(field_name + "=" + bucket.getKey()+" count="+bucket.getDocCount());
        }

    }
    synchronized void change_Elastic_building_in_progress_in_progress(){
            main.Elastic_building_in_progress = 1;
    }
    synchronized int check_Parsing_in_progress(){
        return main.Parsing_in_progress;
    }
    synchronized int check_Downloading_in_progress(){
        return main.Downloading_in_progress;
    }

    void main_elastic_talker(){
        boolean run = true;
        Thread[] elastic_threads = new Thread[threads_number];
        int i = 0;
        int type_of_creating_threads = 1;
        while (run) {
            QueueingConsumer.Delivery delivery;
            try {
                delivery = consumer.nextDelivery();
                if (i < threads_number || elastic_threads.length < threads_number) {
                    elastic_threads[i] = new Thread(new Elastic_talker(type_of_creating_threads, client, channel,
                                                    new String(delivery.getBody()),
                                                    delivery.getEnvelope().getDeliveryTag()));
                    elastic_threads[i].start();
                    i += 1;
                } else {
                    int j = 0;
                    for (Thread thread : elastic_threads) {
                        if (!thread.isAlive()) {
                            break;
                        } else {
                            j += 1;
                        }
                    }
                    if (j >= threads_number) {
                        Thread.sleep(100 + (Thread.currentThread().getId() % 5) * 50);
                    } else {
                        elastic_threads[j] = new Thread(new Elastic_talker(type_of_creating_threads, client, channel,
                                                        new String(delivery.getBody()),
                                                        delivery.getEnvelope().getDeliveryTag()));
                        elastic_threads[j].start();
                    }
                }

                if ((channel.queueDeclare(Queue_name, durable, false, false, map_args).getQueue().length() == 0) &&
                        (check_Downloading_in_progress() <= 0) && (check_Parsing_in_progress() <= 0)) {
                    log.info("I`m dying!!!!!!!!!!!!!!!!!!!!!! " + Thread.currentThread().getName());
                    break;
                }

            } catch (InterruptedException ie) {
                ie.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    void little_elastic_talker() throws IOException, NoSuchAlgorithmException {

        info_for_elastic what_to_put_to_elastic = JSON.parseObject(this.message.getBytes(), info_for_elastic.class);
        channel.basicAck(tag, false);

//            Here will be switch to functions on tag in packet
        switch (what_to_put_to_elastic.what_to_do) {
            case 0:
                log.info("Got url to elastic = " + what_to_put_to_elastic.url);
                pushSomeData(what_to_put_to_elastic);
                break;
            case 1:
                getSomeDataList("author", "Александра");
                break;
            case 2:
                find_dublicates();
                break;
            case 3:
                getSomeDataList("topic","Мариуполя Азовстали");
                break;
            case 4:
                getSomeDataList("url","https://voicesevas.ru/news/");
                break;
            case 5:
                log.info(what_to_put_to_elastic.url);
                switch(what_to_put_to_elastic.url){
                    case "Aggregate_url":
                        getCountAggregation("url");
                        break;
                    case "Aggregate_topic":
                        getCountAggregation("topic");
                        break;
                    case "Aggregate_date":
                        getCountAggregation("date");
                        break;
                    default:
                        getCountAggregation("author");

                }
                break;
            default:
                log.info("Printing HALL DB !!!");
                getSomeDataAll();
        }
    }

    @Override
    public void run() {
        if (thread_type == 0) {

            Config conf = ConfigFactory.load();

            try {
                initialize(conf.getConfig("es"));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (TimeoutException | ExecutionException | InterruptedException e) {
                e.printStackTrace();
            }
            change_Elastic_building_in_progress_in_progress();

            main_elastic_talker();

        } else {

            try {
                little_elastic_talker();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

        }
    }
}
