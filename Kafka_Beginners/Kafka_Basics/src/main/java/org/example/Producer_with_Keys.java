package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer_with_Keys {
    private static final Logger log= LoggerFactory.getLogger(Producer_with_Keys.class.getSimpleName());
    public static void main(String[] args) {


        //create producer properties
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"[::1]:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create producer
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);
        for(int i=0;i<10;i++)
        {
            //create a producer record
            ProducerRecord<String,String> producerRecord=
                    new ProducerRecord<>("second_topic","Id_"+i,"Data with Keys"+i);

            //send data
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception==null){
                        log.info("Received new Metadata/ \n"+
                                "Key: " + producerRecord.key() +"\n"+
                                "Partition" +metadata.partition()+"\n"+
                                "Offset" + metadata.offset()+"\n");
                    }
                    else {
                        log.error("Error",exception);
                    }
                }
            });
        }

        //flush and close producer
        //we use flush cuz the producer will be closed before the data even reached to kafka
        //flush is synchronous it will wait until all data is sent to kafka

        producer.flush();
        producer.close();
    }
}