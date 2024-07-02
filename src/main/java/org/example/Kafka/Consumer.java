package org.example.Kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaSparkContext;
import org.example.Databases.PostgresSQL.PostgresSQL;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

public class Consumer {
    private String topic;
    private String bootstrapServers;
    private String groupId;

    public Consumer(String topic, String bootstrapServers, String groupId) {
        this.topic = topic;
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
    }

    public void start(JavaSparkContext sc, Map<String, Object> job) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("group.id", groupId);
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));

        PostgresSQL postgresSQL = null;
//        if (job.getType_database().equals("postgress")) {
            postgresSQL = new PostgresSQL( job);
//        }


        int cpt = 0;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Message: " + record.value());

//                assert postgresSQL != null;
//                postgresSQL.convertLogToRDD(new int[]{2, 4, 7, 13}, record.value());
            }
        }
    }


}
