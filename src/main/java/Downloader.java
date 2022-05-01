import com.alibaba.fastjson.JSON;
import org.apache.http.HttpEntity;
import org.apache.http.client.CookieStore;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.*;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;


public class Downloader extends Thread  {
    private static Logger log = LogManager.getLogger();
    private CloseableHttpClient client = null;
    private HttpClientBuilder builder;
    private String server = "http://voicesevas.ru/news/";
    private int retryDelay = 5 * 1000;
    private int retryCount = 2;
    private int metadataTimeout = 30*1000;
    private int Parsing_Queue_size;


    private Channel channel;
    String exchangeName;
    String routingKey_dwnl;
    String routingKey_elastic;
    private String message;
    private long tag;
    public int MAX_DEPTH;


     //then go to getUrl where waiting order to download
    public Downloader(Channel channel, String exchangeName, String routingKey_dwnl, String routingKey_elastic,
                      String message, long tag, int MAX_DEPTH) {
        this.channel = channel;
        this.exchangeName = exchangeName;
        this.routingKey_elastic = routingKey_elastic;
        this.routingKey_dwnl = routingKey_dwnl;
        this.message = message;
        this.tag = tag;
        this.MAX_DEPTH = MAX_DEPTH;

        CookieStore httpCookieStore = new BasicCookieStore();
        builder = HttpClientBuilder.create().setDefaultCookieStore(httpCookieStore);
        client = builder.build();
    }


    synchronized void change_Downloading_in_progress(int delta){
        if (delta == 1) {
            main.Downloading_in_progress += 1;
        } else {
            main.Downloading_in_progress -= 1;
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
            log.info("Url " + text_link + " deep added to dwnl queue with depth: " + depth);
        } else {
            log.info("Too deep!");
        }
        change_Parsing_in_progress(0);
        return 1;
    }
    void push_to_elastic_queue(info_for_elastic message) throws IOException {
        byte[] messageBodyBytes = JSON.toJSONString(message).getBytes();
        channel.basicPublish(exchangeName, routingKey_elastic
                ,MessageProperties.PERSISTENT_TEXT_PLAIN, messageBodyBytes);
    }

    public int getUrl() throws IOException {
        info_for_download url_struct;

        url_struct = JSON.parseObject(message.getBytes(), info_for_download.class);
        //log.info("Got url to download = " + url_struct.url);
        channel.basicAck(tag, false);
        if (url_struct.url == null){
            return 0;
        }
        change_Downloading_in_progress(1);

        int code = 0;
        boolean bStop = false;
        Document doc = null;
        for (int iTry = 0; iTry < retryCount && !bStop; iTry++) {
            //log.debug("getting page from url " + url_struct.url);
            RequestConfig requestConfig = RequestConfig.custom()
                    .setSocketTimeout(metadataTimeout)
                    .setConnectTimeout(metadataTimeout)
                    .setConnectionRequestTimeout(metadataTimeout)
                    .setExpectContinueEnabled(true)
                    .build();
            HttpGet request = new HttpGet(url_struct.url);
            request.setConfig(requestConfig);
            CloseableHttpResponse response = null;
            try {
                response = client.execute(request);
                code = response.getStatusLine().getStatusCode();
                if (code == 404) {
                    log.warn("error get url " + url_struct.url + " code " + code);
                    bStop = true;//break;
                } else if (code == 200) {
                    HttpEntity entity = response.getEntity();
                    if (entity != null) {
                        try {
                            doc = Jsoup.parse(entity.getContent(), "UTF-8", server);
                            //log.debug("Downloaded new document by " + Thread.currentThread().getName());
                            break;
                        } catch (IOException e) {
                            log.error(e);
                        }
                    }
                    bStop = true;//break;
                } else {
                    //if (code == 403) {
                    log.warn("error get url " + url_struct.url + " code " + code);
                    response.close();
                    response = null;
                    client.close();
                    CookieStore httpCookieStore = new BasicCookieStore();
                    builder.setDefaultCookieStore(httpCookieStore);
                    client = builder.build();
                    int delay = retryDelay * 1000 * (iTry + 1);
                    log.info("wait " + delay / 1000 + " s...");
                    try {
                        Thread.sleep(delay);
                        continue;
                    } catch (InterruptedException ex) {
                        break;
                    }
                }
            } catch (IOException e) {
                log.error(e);
            }
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    log.error(e);
                }
            }
        }

        ParseNews(doc, url_struct.doc_type, url_struct.url, url_struct.depth);
        return 1;
    }

    public int ParseNews(Document doc, int doc_type, String link, int depth) throws IOException {
        //Get page
        info_for_parse document_struct;

        document_struct = new info_for_parse(doc, doc_type, link, depth);
        //log.info("Got link to parse = " + document_struct.link);
        if (document_struct.link == null){
            return 0;
        } else if (document_struct.doc == null){
            return 0;
        }

        change_Parsing_in_progress(1);

        //Parse page
        if (document_struct.doc_type == 0) { //main page with list of news
            Elements news = document_struct.doc.getElementsByClass("post-blog-list");//.select(".news-item");
            for (Element element : news) {
                try {
                    Element etitle = element.child(0);
                    String text_link = etitle.child(1).child(0).child(0).child(0).attr("href");

                    push_to_download_queue(text_link, 1, document_struct.depth);
                    log.info("Next news from main page: " + text_link + "\n");// + about_link);

                } catch (Exception e) {
                    log.error(e);
                }

            }

//            Elements news_next_page = document_struct.doc.getElementsByClass("navigations");
//            for (Element element : news_next_page) {
//                try {
//                    String text_link = "";
//                    for (Element tag : element.child(1).getElementsByTag("a")){
//                        text_link = tag.attr("href");
//                    }
//                    log.info("Next main page " + text_link + "\n");
//                    push_to_download_queue(text_link, 0, document_struct.depth);
//                } catch (Exception e) {
//                    log.error(e);
//                }
//
//            }


            return 1;
        } else { // page with news
            Elements news = document_struct.doc.getElementsByClass("post-single-details-body");

            String topic = "";
            String date = "";
            String author = "";
            String filling = "";

            topic = news.first().child(0).text();

            Elements get_date = news.select("div.post-meta").select("div.post-meta > ul").select("li");
            for (Element e : get_date) {
                date = e.getElementsByTag("a").text();
                break;
            }

            Elements get_author = news.select("div.post-meta").select("div.post-meta > ul").select("li");
            int li_level = 0;
            for (Element e : get_author) {
                if (li_level == 0 || li_level == 1){
                    li_level++;
                    continue;
                }
                author = e.getElementsByTag("a").text();
                break;
            }

            for (Element e : news){
                Elements get_content = e.child(2).children();
                for (Element e1 : get_content) {
                    filling += e1.text();
                }
                break;
            }

            /*log.info("Page news" + "\n" + "topic: " + topic + "\n"
                    + "Date: " + date + "\n"
                    + "Author: " + author + "\n"
                    + "URL: " + document_struct.link + "\n"
                    + "Filling: " + filling);*/
            push_to_elastic_queue(new info_for_elastic(document_struct.link, topic, date, author, filling, 0));

            for (Element element : news) { //geting new links
                try {
                    Elements other_news_at_same_topic = element.getElementsByClass("recent-wrap");

                    for (Element news_at_same_topic : other_news_at_same_topic) {
                        String text_link = news_at_same_topic.child(0).child(0).attr("href");
//                        String about_link = news_at_same_topic.child(0).child(0).text();

                        push_to_download_queue(text_link, 1, document_struct.depth);
                        log.info("Next news from news page: " + text_link + "\n");// + about_link);
                    }
                    break;
                } catch (Exception e) {
                    log.error(e);
                }

            }
            return 1;
        }

    }

    @Override
    public void run() {
        try {
            getUrl();
            //channel.basicAck(tag, false);
        } catch (IOException e) {
            e.printStackTrace();
            log.info("we are fucked!!!");
        } catch (Exception ex) {
            ex.printStackTrace(System.err);
        }
    }
}
