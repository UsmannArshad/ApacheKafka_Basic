package org.example;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikiMediaChangesProducer {
    public static void main(String[] args) throws InterruptedException {

        //Creating Producer and Properties
        String Bootstrap_Servers="[::1]:9092";
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,Bootstrap_Servers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);

        //Event Handling
        EventHandler eventHandler=new WikiMediaChangeHandler(producer,"wikimedia.recentchange");;
        String url="https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource=builder.build();

        //Start the producer in another thread
        eventSource.start();

        //produce for 10 minutes
        TimeUnit.MINUTES.sleep(10);


    }
}