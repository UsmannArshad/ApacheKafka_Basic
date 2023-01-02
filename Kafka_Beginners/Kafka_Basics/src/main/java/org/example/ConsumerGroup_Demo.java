package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerGroup_Demo {
    private static final Logger log= LoggerFactory.getLogger(ConsumerGroup_Demo.class.getSimpleName());
    public static void main(String[] args) {

        log.info("Hello World");

        //create consumer properties
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"[::1]:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"Group_Id_1");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest"); //latest,none


        //create consumer
        KafkaConsumer<String, String> consumer=new KafkaConsumer<>(properties);

        //Subscribe Consumer to the topic
        consumer.subscribe(Collections.singletonList("second_topic"));  //array.aslist(topic) for multiple topics

        //Poll for new data
        while(true){
            log.info("Pooling");
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord<String,String> record:records){
                log.info("Key"+record.key()+" Value"+record.value());
            }
        }
        //You can also gracefully closed it.Watch video if needed.
    }
}