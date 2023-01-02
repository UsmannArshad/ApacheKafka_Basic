package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer_Demo_Cooperative_Rebalancing {
    private static final Logger log= LoggerFactory.getLogger(Consumer_Demo_Cooperative_Rebalancing.class.getSimpleName());
    public static void main(String[] args) {

        //for multiple consumers related to the same consumer grp edit configuration from the above
        //and check run on multiple instances run the main method 3 times for example
        //then each consumer will get data from one partition each.
        //create consumer properties
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"[::1]:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"Group_Id_1");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest"); //latest,none
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());


        //create consumer
        KafkaConsumer<String, String> consumer=new KafkaConsumer<>(properties);

        //Subscribe Consumer to the topic
        consumer.subscribe(Collections.singletonList("second_topic"));  //array.aslist(topic) for multiple topics

        //Poll for new data
        while(true){
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord<String,String> record:records){
                log.info("Key"+record.key()+" Value"+record.value());
            }
        }
        //You can also gracefully closed it.Watch video if needed.
    }
}