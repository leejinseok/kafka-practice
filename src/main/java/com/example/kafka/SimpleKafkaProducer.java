package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class SimpleKafkaProducer {

    private static final String TOPIC_NAME = "test"; //토픽명


    public static void main(String[] args) throws InterruptedException {

        Random random = new Random();

        Properties prop = new Properties();
        prop.put("bootstrap.servers", "localhost:9092"); // server, kafka host
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("acks", "all");
        prop.put("block.on.buffer.full", "true");

        String message = null;

        // producer 생성
        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);

        // message 전달
        while (true) {
            message = Integer.toString(random.nextInt(100)); // 1~100 중 랜덤숫자
            producer.send(new ProducerRecord<String, String>(TOPIC_NAME, message));
            Thread.sleep(1000); // 1초
        }

    }

}
