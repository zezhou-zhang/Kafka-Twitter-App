package com.kafka.streaming;

import com.kafka.streaming.consumer.TwitterConsumer;
import com.kafka.streaming.producer.Producer;
import com.kafka.streaming.producer.TwitterProducer;
import com.google.common.collect.Lists;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.List;

public class KafkaApp {
    private static final KafkaProducer<String, String> kafkaProducer = Producer.createKafkaProducer();
    private static final List<String> trackTerms = Lists.newArrayList("coronavirus", "coronavirus vaccine");
    public static final List<String> topicList = Lists.newArrayList("covid-tweets","vaccine-tweets");
    public static void main(String[] args) {
        try {
            // 1. First Run Producer to produce Twitter tweets to Kafka Topic
            for (int i = 0; i < trackTerms.size(); i++){
                new TwitterProducer(kafkaProducer, trackTerms.get(i), topicList.get(i)).start();
            }

            // 2. Uncomment it to run Kafka producer and subscribe to Twitter Kafka Topic
            new TwitterConsumer();
            // 3. Run Kafka producer, subscribe to Twitter Kafka topic and ingest data into MongoDB database
            // new KafkaConsumerMongoDb();
        }catch(Exception e){
            System.out.println(e.getStackTrace().getClass());
        }
    }

}
