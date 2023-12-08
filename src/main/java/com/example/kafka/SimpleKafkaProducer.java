package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class SimpleKafkaProducer {

    private static final String TOPIC_NAME = "test"; //토픽명


    public static void main(String[] args) throws InterruptedException {
        Random random = new Random();

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");

        String message = null;

        // producer 생성
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // message 전달
        while (true) {
            message = Integer.toString(random.nextInt(100)); // 1~100 중 랜덤숫자
            producer.send(new ProducerRecord<>(TOPIC_NAME, message));
            Thread.sleep(1000); // 1초
        }

    }

}
