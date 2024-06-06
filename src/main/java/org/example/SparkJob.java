package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.example.Database.PostgresSQL.PostgresSQL;
import org.example.Kafka.Consumer;
import org.example.Kafka.ConsumerThread;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.io.Serializable;
import java.util.*;

public class SparkJob implements Job, Serializable {
    int[] index = {2, 4, 7, 13};

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {


        System.out.println("Current system time when job starts: " + new Date());
//
//
//        SparkConf conf = new SparkConf()
//                .setAppName("logs")
//                .setMaster("local[*]");
//        Map<String, Object> job = (Map<String, Object>) context.getMergedJobDataMap().get("job_spark");
//        String bootstrapServers = context.getMergedJobDataMap().getString("bootstrapServers");
////        String topic = job.get("type");
//        String topic = "postgress";
//
//        JavaSparkContext sc = new JavaSparkContext(conf);
//
//
//
//
//            String groupId1 = "group-" + job.get("id") + "-1";
//            Consumer consumer1 = new Consumer(topic, bootstrapServers, groupId1);
//            ConsumerThread consumerThread1 = new ConsumerThread(consumer1, sc, job);
//            Thread thread1 = new Thread(consumerThread1);
//            thread1.start();
//
//            String groupId2 = "group-" + job.getId() + "-2";
//            Consumer consumer2 = new Consumer(topic, bootstrapServers, groupId2);
//            ConsumerThread consumerThread2 = new ConsumerThread(consumer2, sc, job);
//            Thread thread2 = new Thread(consumerThread2);
//            thread2.start();



        // Create and start second consumer thread


//        String groupId = "group-" + job.getId();
//        Consumer consumer1 = new Consumer(topic, bootstrapServers, groupId);
//        ConsumerThread consumerThread1 = new ConsumerThread(consumer1, sc, job);
//        Thread thread1 = new Thread(consumerThread1);
//        thread1.start();




        // Start Consumer with the unique groupId
//        Consumer consumer = new Consumer(topic, bootstrapServers, groupId);
//        new Thread(() -> consumer.start(sc, job)).start();

//
//            Consumer consumer  = new Consumer(topic,bootstrapServers);
//            consumer.start(sc , job);

        // Fetching job details from context

        SparkConf conf = new SparkConf()
                .setAppName("logs")
                .setMaster("local[*]");

        try {
            JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(2));
        Map<String, Object> job = (Map<String, Object>) context.getMergedJobDataMap().get("job_spark");
            String topic = "postgress";
            String bootstrapServers = context.getMergedJobDataMap().getString("bootstrapServers");
            JavaSparkContext sc = streamingContext.sparkContext();

            System.out.println("JavaStreamingContext initialized successfully");

            Map<String, Object> kafkaParams = new HashMap<>();
            kafkaParams.put("bootstrap.servers", bootstrapServers);
            kafkaParams.put("key.deserializer", StringDeserializer.class.getName());
            kafkaParams.put("value.deserializer", StringDeserializer.class.getName());
            kafkaParams.put("group.id", "group1");
            kafkaParams.put("enable.auto.commit", "true");
            kafkaParams.put("auto.commit.interval.ms", "1000");
            kafkaParams.put("auto.offset.reset", "latest");

            Collection<String> topics = Arrays.asList(topic);

            JavaInputDStream<ConsumerRecord<String, String>> stream =
                    KafkaUtils.createDirectStream(
                            streamingContext,
                            LocationStrategies.PreferConsistent(),
                            ConsumerStrategies.Subscribe(topics, kafkaParams)
                    );
            PostgresSQL postgresSQL = new PostgresSQL(sc, job);


            // Processing each RDD partition
            stream.foreachRDD(rdd -> {
                rdd.foreachPartition(records -> {


                    while (records.hasNext()) {
                        ConsumerRecord<String, String> record = records.next();
                        System.out.println("Received message: " + record.value());
//                        postgresSQL.convertLogToRDD(index, record.value());
                    }


                });
            });

            streamingContext.start();
            streamingContext.awaitTermination();

            System.out.println("Job completed: " );
        } catch (Exception e) {
            e.printStackTrace();
            throw new JobExecutionException("Error initializing JavaStreamingContext", e);
        }
    }
}
