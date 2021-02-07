package com.kafka.streaming.consumer;
import com.fasterxml.jackson.databind.JsonNode;
import com.kafka.streaming.config.KafkaConfig;
import com.kafka.streaming.json.Json;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class TwitterConsumer {

//    public static void main(String[] args) {
//        new TwitterConsumer().run();
//    }
    private final String bootstrapServers = KafkaConfig.BOOTSTRAPSERVERS;
    private final String groupId = "my-first-app";
    private List<String> topics;
    private int maxNumberofTweets;

    public TwitterConsumer(List<String> topics, int maxNumberofTweets) {
        this.topics = topics;
        this.maxNumberofTweets = maxNumberofTweets;
        run();
    }
    private void run() {
        Logger logger = LoggerFactory.getLogger(TwitterConsumer.class.getName());
        // latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        // create the consumer runnable
        logger.info("Creating the consumer thread");
        Runnable myConsumerRunnable = new ConsumerRunnable(
                bootstrapServers,
                groupId,
                topics,
                latch
        );

        // start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info(" ---- Application has exited ---- ");
        }

        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info(" ---- Application is closing ---- ");
        }
    }

    public class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String,String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
        private volatile int maxRetweetCountforCovid = 0;
        private volatile int maxRetweetCountforVaccine = 0;
        private volatile String maxRetweetedTweet;

        public ConsumerRunnable(String bootstrapServers,
                                String groupId,
                                List<String> topic,
                                CountDownLatch latch) {
            this.latch = latch;

            // create consumer configs
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // create consumer
            consumer = new KafkaConsumer<String, String>(properties);
            // subscribe consumer to our topic(s)
            consumer.subscribe(topic);
        }

        @Override
        public void run() {
            // poll for new data
            try {
                int tweetCounter = 0;
                while (tweetCounter < maxNumberofTweets) {
                    ConsumerRecords<String, String> records =
                            consumer.poll(Duration.ofSeconds(1)); // new in Kafka 2.0.0

                    for (ConsumerRecord<String, String> record : records) {
                        //logger.info("Key-> " + record.key() + ", Value->: " + record.value());
                        //logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
                        JsonNode node = null;
                        JsonNode subNode = null;
                        int retweetCount;
                        try {
                            long offset = record.offset();
                            String kafkaTopic = record.topic();
                            node = Json.parse(record.value());
                            String tweetText = node.get("text").asText();
                            System.out.println("Current tweet Text: " + tweetText);
                            JsonNode quotedStatusNode = node.get("quoted_status");
                            if (quotedStatusNode!= null) {
                                retweetCount = quotedStatusNode.get("retweet_count").asInt();
                            }else {
                                retweetCount = node.get("retweet_count").asInt();
                            }

                            if(kafkaTopic.equals("covid-tweets-cluster")){
                                if (retweetCount >= maxRetweetCountforCovid) {
                                    maxRetweetCountforCovid = retweetCount;
                                    maxRetweetedTweet = record.value().toString();
                                    System.out.println(String.format("ID: d% Tweet with retweet count: %d is received from the topic: %s",
                                            offset, maxRetweetCountforCovid, kafkaTopic));
                                    System.out.println(tweetText + "\n");
                                }
                            }else{
                                if (retweetCount >= maxRetweetCountforVaccine) {
                                    maxRetweetCountforVaccine = retweetCount;
                                    maxRetweetedTweet = record.value().toString();
                                    System.out.println(String.format("ID: d% Tweet with retweet count: %d is received from the topic: %s",
                                            offset, maxRetweetCountforVaccine, kafkaTopic));
                                    System.out.println(tweetText + "\n");
                                }
                            }
                            consumer.commitAsync();
                            tweetCounter++;
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            } catch (WakeupException e) {
                logger.info(" ----- Received shutdown signal! ----- ");
            } finally {
                consumer.close();
                // tell our main code we're done with the consumer
                latch.countDown();
            }
        }

        public void shutdown() {
            // the wakeup() method is a special method to interrupt consumer.poll()
            // it will throw the exception WakeUpException
            consumer.wakeup();
        }
    }

}
