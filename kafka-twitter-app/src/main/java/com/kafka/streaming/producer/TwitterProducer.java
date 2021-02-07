package com.kafka.streaming.producer;

import com.google.common.collect.Lists;
import com.kafka.streaming.config.TwitterConfig;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer extends Thread {

    final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    private Client client;
    private KafkaProducer<String, String> kafkaProducer;
    private BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(30);
    private List<String> trackTermList;
    private String kafkaTopic;
    private int maxNumberofTweets;


    public TwitterProducer(KafkaProducer<String, String> kafkaProducer, String trackTerm, String kafkaTopic, int maxNumberofTweets) {
        this.kafkaProducer = kafkaProducer;
        this.trackTermList =  Lists.newArrayList(trackTerm);
        this.kafkaTopic = kafkaTopic;
        this.maxNumberofTweets = maxNumberofTweets;
    }

    public void start(){
        Thread thread = new Thread(this);
        thread.setName(kafkaTopic);
        thread.start();
    }

    public void run(){
        logger.info("Setting up");

        // 1. Call the Twitter Client
        client = createTwitterClient(trackTermList, msgQueue);
        client.connect();

        // Shutdown Hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Application is not stopping!");
            client.stop();
            logger.info("Closing Producer");
            kafkaProducer.close();
            logger.info("Finished closing");
        }));

        // 3. Send Tweets to Kafka
        int numberOfSentTweets = 0;
        while (!client.isDone() && numberOfSentTweets < maxNumberofTweets) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                logger.info(msg);
                kafkaProducer.send(new ProducerRecord<String, String>(kafkaTopic, null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        System.out.println(String.format("Record was sent to topic %s with partition: %d, offset: %d",
                                recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset()));
                        if (e != null) {
                            logger.error("Some error OR something bad happened", e);
                        }
                    }
                });
                numberOfSentTweets++;
            }
        }
        logger.info("\n Application End");
    }

    public Client createTwitterClient(List<String> trackTerms, BlockingQueue<String> msgQueue) {
        /** Setting up a connection   */
        Hosts twitterHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hbEndpoint = new StatusesFilterEndpoint();
        // Term that I want to search on Twitter
        hbEndpoint.trackTerms(trackTerms);
        // Twitter API and tokens
        Authentication hosebirdAuth = new OAuth1(TwitterConfig.CONSUMER_KEYS, TwitterConfig.CONSUMER_SECRETS, TwitterConfig.TOKEN, TwitterConfig.SECRET);

        /** Creating a client   */
        ClientBuilder builder = new ClientBuilder()
                .name("Kafka-Twitter-Client")
                .hosts(twitterHosts)
                .authentication(hosebirdAuth)
                .endpoint(hbEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hbClient = builder.build();
        return hbClient;
    }


}