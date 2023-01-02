package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {
    private static final Logger log= LoggerFactory.getLogger(Producer.class.getSimpleName());
    public static void main(String[] args) {

        log.info("Hello World");

        //create producer properties
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"[::1]:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create producer
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);

        //create a producer record
        ProducerRecord<String,String> producerRecord=
                new ProducerRecord<>("first_topic","AssalamoAlaikum");

        //send data
        producer.send(producerRecord);

        //flush and close producer
        //we use flush cuz the producer will be closed before the data even reached to kafka
        //flush is synchronous it will wait until all data is sent to kafka

        producer.flush();
        producer.close();
    }
}