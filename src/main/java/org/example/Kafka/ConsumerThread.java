package org.example.Kafka;

import org.apache.spark.api.java.JavaSparkContext;
import org.example.Job;

import java.util.Map;

public class ConsumerThread implements Runnable {
    private Consumer consumer;
    private JavaSparkContext sc;
    private Map<String, Object> job;

    public ConsumerThread(Consumer consumer, JavaSparkContext sc, Map<String, Object> job) {
        this.consumer = consumer;
        this.sc = sc;
        this.job = job;
    }

    @Override
    public void run() {
        consumer.start(sc, job);
    }
}

// In your main method or wherever you create consumers:

//    Consumer consumer1 = new Consumer(topic, bootstrapServers, "group1");
//    ConsumerThread consumerThread1 = new ConsumerThread(consumer1, sc, job);
//    Thread thread1 = new Thread(consumerThread1);
//thread1.start();
//
//        Consumer consumer2 = new Consumer(topic, bootstrapServers, "group2");
//        ConsumerThread consumerThread2 = new ConsumerThread(consumer2, sc, job);
//        Thread thread2 = new Thread(consumerThread2);
//        thread2.start();
