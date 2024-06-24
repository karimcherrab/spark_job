package org.example.Kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Producer {
    String bootstrapServers , topic ;

    public Producer(String bootstrapServers, String topic) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
    }

    public void send(String message){
//        String bootstrapServers = "localhost:9092";
//        final String topic = "topic01";

        Properties properties = new Properties();
        properties.put("client.id","basic-producer-v0.1.0");
        properties.put("bootstrap.servers",bootstrapServers);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        org.apache.kafka.clients.producer.Producer<String, String> producer = new KafkaProducer<String, String>(properties);

//        for (int i = 1  ; i<5  ; i++){
//            String key = "key-"+i;
//            String val = "val-"+i;
        ProducerRecord<String , String> p = new ProducerRecord<>(topic , "key-111" , message);
        producer.send(p);
        //}

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            producer.close();
        }));
    }
}
