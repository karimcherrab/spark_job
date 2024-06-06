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

    void producer(){
//        String bootstrapServers = "localhost:9092";
//        final String topic = "topic01";

        Properties properties = new Properties();
        properties.put("client.id","basic-producer-v0.1.0");
        properties.put("bootstrap.servers",bootstrapServers);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        org.apache.kafka.clients.producer.Producer<String, String> producer = new KafkaProducer<String, String>(properties);

        String value = "2023-09-27 00:10:34.874 UTC,\"devops\",\"manage_display_prod\",34150,\"172.16.60.101:59384\",65136c12.8566,3,\"SELECT\",2023-09-26 23:41:06 UTC,60/24801198,0,LOG,00000,\"execute S_5: select offer0_.offer_id as offer_id1_6_, offer0_.offer_code as offer_co2_6_, offer0_.offer_name as offer_na3_6_, offer0_.position as position4_6_, offer0_.price as price5_6_, offer0_.type_id as type_id6_6_, offer0_.validity_id as validity7_6_ from offer offer0_\",,,,,,,,,\"PostgreSQL JDBC Driver\",\"client backend\"";
//        for (int i = 1  ; i<5  ; i++){
//            String key = "key-"+i;
//            String val = "val-"+i;
        ProducerRecord<String , String> p = new ProducerRecord<>(topic , "key-111" , value);
        producer.send(p);
        //}

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            producer.close();
        }));
    }
}
