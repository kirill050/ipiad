import com.alibaba.fastjson.JSON;
import org.apache.http.client.CookieStore;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.*;



public class Parser  implements Runnable {
    private static Logger log = LogManager.getLogger();
    private static Downloader downloader;
    private static String web_address = "http://voicesevas.ru/news/";
    public static int Parsing_in_progress = 0;
    public static int Downloading_in_progress = 0;
    public int MAX_DEPTH;
    private int Downloading_Queue_size;

    private Channel channel;
    String exchangeName;
    String routingKey_dwnl;
    private String message;
    private long tag;

    public Parser(Channel channel, String exchangeName, String routingKey_dwnl, String message, long tag, int MAX_DEPTH){
        this.channel = channel;
        this.exchangeName = exchangeName;
        this.routingKey_dwnl = routingKey_dwnl;
        this.message = message;
        this.tag = tag;
        this.MAX_DEPTH = MAX_DEPTH;
    }

    @Override
    public void run() {
        try {
            ParseNews();
            channel.basicAck(tag, false);
        } catch (IOException e) {
            e.printStackTrace();
            log.info("we are fucked!!!");
        } catch (Exception ex) {
            ex.printStackTrace(System.err);
        }
    }

    synchronized void change_Parsing_in_progress(int delta){
        if (delta == 1) {
            main.Parsing_in_progress += 1;
        } else {
            main.Parsing_in_progress -= 1;
        }
    }

    private int push_to_download_queue(String text_link, int doc_type, int depth) throws IOException {
        if (depth < MAX_DEPTH) {
            info_for_download temp = new info_for_download(text_link, doc_type, (depth+1));
            //byte[] messageBodyBytes =  temp.toString().getBytes();
            byte[] messageBodyBytes = JSON.toJSONString(temp).getBytes();
            channel.basicPublish(exchangeName, routingKey_dwnl
                    ,MessageProperties.PERSISTENT_TEXT_PLAIN, messageBodyBytes);
            log.info("Url " + text_link + "added to Downloading_Queue by " + Thread.currentThread().getName());
            //log.info("Downloading_Queue.size() = " + Downloading_Queue.size() + " DEPTH = " + depth + temp.depth);
        } else {
           log.info("Too deep!");
        }
        change_Parsing_in_progress(0);
        return 1;
    }

    public int ParseNews() {
        //Get page
        info_for_parse document_struct;

        //document_struct = (info_for_parse)message.getBytes();
        document_struct = JSON.parseObject(message.getBytes(), info_for_parse.class);
        log.info("Got link to parse = " + document_struct.link);
        if (document_struct.link == null){
            return 0;
        }

        change_Parsing_in_progress(1);

        //Parse page
        if (document_struct.doc_type == 0) { //main page with list of news
            Elements news = document_struct.doc.getElementsByClass("base shortstory");//.select(".news-item");
            for (Element element : news) {
                try {
                    Element etitle = element.child(0);
                    String text_link = etitle.child(2).attr("href");
                    String about_link = "";
                    for (Element child : etitle.child(3).children()) {
                        about_link += child.text();
                    }
                    push_to_download_queue(text_link, 1, document_struct.depth);
                    log.info("Next news from main page: " + text_link + "\n" + about_link);
                    log.debug("Next news from main page: " + text_link + "\n" + about_link);

                } catch (Exception e) {
                    log.error(e);
                }

            }

            Elements news_next_page = document_struct.doc.getElementsByClass("dpad basenavi ignore-select");
            for (Element element : news_next_page) {
                try {
                    String text_link = "";
                    for (Element tag : element.child(0).child(0).getElementsByTag("a")){
                        text_link = tag.attr("href");
                    }
                    log.debug("Next main page " + text_link + "\n");
                    push_to_download_queue(text_link, 0, document_struct.depth);
                } catch (Exception e) {
                    log.error(e);
                }

            }
            return 1;
        } else { // page with news
            Elements news = document_struct.doc.getElementsByClass("base fullstory");

            String topic = "";
            String date = "";
            String author = "";
            String filling = "";

            Elements get_topic = news.select("div.dpad").select("h1.btl");
            for (Element e : get_topic) {
                topic = e.text();
                break;
            }

            Elements get_date = news.select("div.dpad").select("div.bhinfo").first().getElementsByTag("a");
            for (Element e : get_date) {
                date = e.text();
                break;
            }
            Elements get_content = news.select("div.dpad").select("div.maincont");
            for (Element e : get_content) {
                filling += e.text();
            }
            log.info("Page news" + "\n" + "topic: " + topic + "\n"
                    + "Date: " + date + "\n"
                    + "URL: " + document_struct.link
                    + "Filling: " + filling);


            for (Element element : news) { //geting new links
                try {
                    Elements other_news_at_same_topic = element.select("div.related").select("div.related > ul").select("li");

                    for (Element news_at_same_topic : other_news_at_same_topic) {
                        String text_link = news_at_same_topic.child(0).child(0).attr("href");
                        String about_link = news_at_same_topic.child(0).child(0).text();

                        push_to_download_queue(text_link, 1, document_struct.depth);
                        log.info("Next news from news page: " + text_link + "\n" + about_link);
                    }
                    break;
                } catch (Exception e) {
                    log.error(e);
                }

            }
            return 1;
        }

    }

}
