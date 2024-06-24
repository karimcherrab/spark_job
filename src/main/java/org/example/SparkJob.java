package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.example.Database.Cassandra.Cassandra;
import org.example.Database.DatabaseType;
import org.example.Database.PostgresSQL.PostgresSQL;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class SparkJob implements Job {
    private static final Map<Integer, AtomicBoolean> jobStopFlags = new HashMap<>();
    private int[] index = {5};

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        // Get job details from context
        Map<String, Object> job = (Map<String, Object>) context.getJobDetail().getJobDataMap().get("job_spark");
        String bootstrapServers = (String) context.getJobDetail().getJobDataMap().get("bootstrapServers");
        String timeEnd = (String) context.getJobDetail().getJobDataMap().get("timeEnd");
        String dateEnd = (String) context.getJobDetail().getJobDataMap().get("dateEnd");

        // Initialize stop flag for this job
        int jobId = (int) job.get("id");
        AtomicBoolean stopFlag = new AtomicBoolean(false);
        jobStopFlags.put(jobId, stopFlag);

        // Start Kafka consumer
        startKafkaConsumerForJob(job, bootstrapServers, dateEnd, timeEnd, stopFlag);
    }

    private void startKafkaConsumerForJob(Map<String, Object> job, String bootstrapServers,
                                          String dateEnd, String timeEnd, AtomicBoolean stopFlag) {
        new Thread(() -> {
            String groupId = "consumer_group_" + job.get("id");
            String topic = (String) job.get("type");
            System.out.println("topic = " +topic);
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList(topic));

            DatabaseType databaseType ;

            if (topic.equals("postgress")){
                databaseType = new PostgresSQL(job);
                index = new int[]{2, 4, 7, 13};

            }else {
                databaseType = new Cassandra(job);
                index = new int[]{5, 7};


            }


            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

            LocalDateTime endDateTime = LocalDateTime.parse(dateEnd.substring(0 , 10) + " " + timeEnd, dateTimeFormatter);

            try {
                while (!stopFlag.get() && LocalDateTime.now().isBefore(endDateTime)) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord<String, String> record : records) {
                        if (stopFlag.get()) {
                            break;
                        }
                        System.out.printf("Consumed record with key %s and value %s, from topic %s%n",
                                record.key(), record.value(), record.topic());
                         databaseType.convertLogToRDD(index, record.value());
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                consumer.close();
                System.out.println("Kafka consumer stopped.");
            }
        }).start();
    }

    public static void stopJob(int jobId) {
        AtomicBoolean stopFlag = jobStopFlags.get(jobId);
        if (stopFlag != null) {
            stopFlag.set(true);
        }
    }
}
