import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Rabbit_endpoint extends Thread{

    synchronized int check_Parsing_in_progress(){
        return main.Parsing_in_progress;
    }
    synchronized int check_Downloading_in_progress(){
        return main.Downloading_in_progress;
    }

    String Queue_name = "download_Queue";
    String exchangeName = "Exchange_btw_threads";
    String routingKey_dwnl = "Route_to_download";
    String routingKey_prs = "Route_to_parse";
    String routingKey_elastic = "Route_to_elastic";
    int threads_number = 5;
    private static Logger log = LogManager.getLogger();
    int MAX_DEPTH = -1;

    synchronized void change_Elastic_building_in_progress_in_progress(){
        main.Elastic_building_in_progress = 2;
    }

    public Rabbit_endpoint(String exchangeName, String Queue_name, String routingKey_dwnl, String routingKey_prs, int threads_number){
        this.exchangeName = exchangeName;
        this.Queue_name = Queue_name;
        this.routingKey_dwnl = routingKey_dwnl;
        this.routingKey_prs = routingKey_prs;
        this.threads_number = threads_number;
    }

    public Rabbit_endpoint(String exchangeName, String Queue_name, String routingKey_dwnl, String routingKey_prs, int threads_number, int MAX_DEPTH){
        this.exchangeName = exchangeName;
        this.Queue_name = Queue_name;
        this.routingKey_dwnl = routingKey_dwnl;
        this.routingKey_prs = routingKey_prs;
        this.threads_number = threads_number;
        this.MAX_DEPTH = MAX_DEPTH;
    }

    void Downloader_endpoint(){
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setUsername("rabbitmq");
            factory.setPassword("rabbitmq");
            factory.setVirtualHost("/");
            factory.setHost("127.0.0.1");
            factory.setPort(5672);
            Connection conn = factory.newConnection();
            Channel channel = conn.createChannel();
            boolean durable = true;
            Map<String, Object> map_args = new HashMap<String, Object>();
            channel.exchangeDeclare(exchangeName, "direct", durable);
            map_args.put("x-max-length", 100);
            channel.queueDeclare(Queue_name, durable, false, false, map_args);
            channel.queueBind(Queue_name, exchangeName, routingKey_dwnl);
            channel.queuePurge(Queue_name);
            change_Elastic_building_in_progress_in_progress();

            QueueingConsumer consumer = new QueueingConsumer(channel);
            channel.basicConsume(Queue_name, false, consumer);
            boolean run = true;

            Thread[] downloader_threads = new Thread[threads_number];
            int i = 0;

            while (run) {
                QueueingConsumer.Delivery delivery;
                try {
                    delivery = consumer.nextDelivery();

                    if (i < threads_number || downloader_threads.length < threads_number) {
                        downloader_threads[i] = new Thread(new Downloader(channel, exchangeName, routingKey_dwnl,
                                routingKey_elastic, new String(delivery.getBody()), delivery.getEnvelope().getDeliveryTag(),
                                MAX_DEPTH));
                        downloader_threads[i].start();
                        i += 1;
                    } else {
                        int j = 0;
                        for (Thread thread : downloader_threads) {
                            if (!thread.isAlive()) {
                                break;
                            } else {
                                j += 1;
                            }
                        }
                        if (j >= threads_number) {
                            Thread.sleep(100 + (Thread.currentThread().getId() % 5) * 50);
                        } else {
                            downloader_threads[j] = new Thread(new Downloader(channel, exchangeName, routingKey_dwnl,
                                    routingKey_elastic, new String(delivery.getBody()), delivery.getEnvelope().getDeliveryTag(), MAX_DEPTH));
                            downloader_threads[j].start();
                        }
                    }

                    if ((channel.queueDeclare(Queue_name, durable, false, false, map_args).getQueue().length() == 0) &&
                            (check_Downloading_in_progress() <= 0) && (check_Parsing_in_progress() <= 0)) {
                        log.info("I`m dying!!!!!!!!!!!!!!!!!!!!!! " + Thread.currentThread().getName());
                        break;
                    }
                } catch (InterruptedException ie) {
                    continue;
                }
            }

            channel.close();
            conn.close();
        } catch (IOException e) {

        } catch (TimeoutException e) {

        }
    }



    @Override
    public void run() {
            Downloader_endpoint();
    }
}
