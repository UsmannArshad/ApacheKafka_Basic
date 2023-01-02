package org.example;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikiMediaChangeHandler implements EventHandler {
    KafkaProducer<String,String> producer;
    String Topic;
    private final Logger log= LoggerFactory.getLogger(WikiMediaChangeHandler.class.getSimpleName());
    public WikiMediaChangeHandler(KafkaProducer<String,String> kafkaProducer, String topic)
    {
        this.producer=kafkaProducer;
        this.Topic=topic;
    }
    @Override
    public void onOpen(){
        //Nothing
    }

    @Override
    public void onClosed(){
        producer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent){
        log.info(messageEvent.getData());
        producer.send(new ProducerRecord<>(Topic,messageEvent.getData()));
    }

    @Override
    public void onComment(String comment){

    }

    @Override
    public void onError(Throwable t) {
        log.error("Error ",t);
    }
}
