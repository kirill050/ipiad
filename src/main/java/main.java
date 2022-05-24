import com.alibaba.fastjson.JSON;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.*;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


public class main {
    private static Logger log = LogManager.getLogger();
    private static String web_address = "http://voicesevas.ru/news/";
    public static int Parsing_in_progress = 0;
    public static int Downloading_in_progress = 0;
    public static int Elastic_building_in_progress = 0; // 1 -- Elastic is ready 2 -- RabbitMQ is ready
    static String routingKey_dwnl = "Route_to_download";
    static String exchangeName = "Exchange_btw_threads";
    static String routingKey_elastic = "Route_to_elastic";
    static int threads_number = 5;
    static int MAX_DEPTH = 10;

    static void put_to_q_other_main_pages(Channel channel) throws IOException {
        //https://voicesevas.ru/page/3/
        for (int i = 2; i <= MAX_DEPTH; i++) {
            String text_link = "https://voicesevas.ru/page/" + String.valueOf(i) + "/";
            log.info("Next main page " + text_link + "\n");

            info_for_download temp = new info_for_download(text_link, 0, (i - 1));

            byte[] messageBodyBytes = JSON.toJSONString(temp).getBytes();
            channel.basicPublish(exchangeName, routingKey_dwnl
                    , MessageProperties.PERSISTENT_TEXT_PLAIN, messageBodyBytes);
        }
    }

    static void filling_Elastic(Channel channel, String exchangeName, Thread esTalker_Thread) throws IOException, InterruptedException {
        //starting threads

        Thread downloaderThread = new Thread(new Rabbit_endpoint("Exchange_btw_threads", "download_Queue",
                "Route_to_download","Route_to_parse", threads_number, MAX_DEPTH));
        downloaderThread.setName("downloaderThread");

        downloaderThread.start();
        log.info("Thread " + downloaderThread.getName() + " with id " + downloaderThread.getId() + " started!");

        while(Elastic_building_in_progress != 2){
            Thread.sleep(50);
        }


        info_for_download temp = new info_for_download(web_address, 0, 0);
        byte[] messageBodyBytes = JSON.toJSONString(temp).getBytes();
        channel.basicPublish(exchangeName, routingKey_dwnl
                ,MessageProperties.PERSISTENT_TEXT_PLAIN, messageBodyBytes);

        put_to_q_other_main_pages(channel);


        int back_count = 100;
        while(downloaderThread.isAlive() && esTalker_Thread.isAlive()){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
            }
            if (!downloaderThread.isAlive()){
                break;
            }
            if (back_count == 0){
                temp = new info_for_download(web_address, 0, 0);
                messageBodyBytes = JSON.toJSONString(temp).getBytes();
                channel.basicPublish(exchangeName, routingKey_dwnl
                        ,MessageProperties.PERSISTENT_TEXT_PLAIN, messageBodyBytes) ;
            } else {
                back_count -= 1;
            }
        }
    }

    static void send_to_Elastic_talker(Channel channel, String url, String topic, String date, String author, String filling, int answer) throws IOException {
        byte[] messageBodyBytes = JSON.toJSONString(new info_for_elastic(url, topic, date,
                author, filling, answer)).getBytes();
        channel.basicPublish(exchangeName, routingKey_elastic
                , MessageProperties.PERSISTENT_TEXT_PLAIN, messageBodyBytes);
    }

    static void just_working_with_queries(Channel channel, String exchangeName, Scanner console) throws IOException {
        System.out.println("What query you want to do? 0 -- quit; 1 -- author query 2 -- Get_duplicates \" +\n" +
                "                    \"3 -- topic query 4 -- url query 5 -- aggregation query 6 -- print all");
        //to check the DB filling
        int answer = 0;
        answer = console.nextInt();
        while(answer != 0){
            switch (answer) {
                case 0:
                    break;
                case 5:
                    System.out.println("By what field aggregate? 1 -- url 2 -- topic " +
                            "3 -- date    4 -- author");
                    switch (console.nextInt()){
                        case 1: //url Aggregate_url
                            send_to_Elastic_talker(channel, "Aggregate_url", "K", "K",
                                    "K", "K", answer);
                            break;
                        case 2://url Aggregate_topic
                            send_to_Elastic_talker(channel, "Aggregate_topic", "K", "K",
                                    "K", "K", answer);
                            break;
                        case 3://url Aggregate_date
                            send_to_Elastic_talker(channel, "Aggregate_date", "K", "K",
                                    "K", "K", answer);
                            break;
                        default://url Aggregate_author
                            send_to_Elastic_talker(channel, "Aggregate_author", "K", "K",
                                    "K", "K", answer);
                    }
                    break;
                default:
                    send_to_Elastic_talker(channel, "Query", "K", "K",
                            "K", "K", answer);
            }
            System.out.println("What query you want to do? 0 -- quit; 1 -- author query 2 -- Get_duplicates " +
                    "3 -- topic query 4 -- url query 5 -- aggregation query 6 -- print all");
            answer = console.nextInt();
        }
    }

    public static void main(String[] Args) throws Exception {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("rabbitmq");
        factory.setPassword("rabbitmq");
        factory.setVirtualHost("/");
        factory.setHost("127.0.0.1");
        factory.setPort(5672);
        Connection conn = factory.newConnection();
        Channel channel = conn.createChannel();


        Thread esTalker_Thread = new Thread(new Elastic_talker(0));
        esTalker_Thread.setName("elasticThread");
        esTalker_Thread.start();
        log.info("Thread " + esTalker_Thread.getName() + " with id " + esTalker_Thread.getId() + " started!");

        while(Elastic_building_in_progress == 0){
            Thread.sleep(50);
        }


        System.out.println("Is this the first try after elastic restart? 0 -- yes; 1 -- no");
        Scanner console = new Scanner(System.in);
        int type_of_programm_start = console.nextInt();

        if (type_of_programm_start == 0){
            filling_Elastic(channel, exchangeName, esTalker_Thread);
        } else { // just working with java query
            just_working_with_queries(channel, exchangeName, console);

            esTalker_Thread.join();
        }



        channel.close();
        conn.close();
        return;
    }
}
