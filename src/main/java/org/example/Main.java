package org.example;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class Main {
    static String topic  = "postgress3";
    static String bootstrapServers  = "localhost:9093";


    public static void main(String[] args) throws ExecutionException, InterruptedException {



        Data data = new Data();
        List<Map<String, Object>> jobs = data.get_jobs();
        System.out.println(jobs);
        for (Map<String, Object> job : jobs) {
            System.out.println("Job ID: " + job.get("id"));
            System.out.println("Job Name: " + job.get("name"));
            System.out.println("Is Active: " + job.get("is_active"));
            System.out.println("Created At: " + job.get("created_at"));

            // Print databases
            List<Map<String, Object>> databases = (List<Map<String, Object>>) job.get("databases");
            for (Map<String, Object> database : databases) {
                System.out.println("\tDatabase ID: " + database.get("database_id"));
                System.out.println("\tDatabase Name: " + database.get("database_name"));

                // Print tables
                List<Map<String, Object>> tables = (List<Map<String, Object>>) database.get("tables");
                for (Map<String, Object> table : tables) {
                    System.out.println("\t\tTable ID: " + table.get("table_id"));
                    System.out.println("\t\tTable Name: " + table.get("table_name"));

                    // Print columns
                    List<Map<String, Object>> columns = (List<Map<String, Object>>) table.get("columns");
                    for (Map<String, Object> column : columns) {
                        System.out.println("\t\t\tColumn Name: " + column.get("column_name"));
                    }
                }
            }

            // Print conf data
            Map<String, Object> conf = (Map<String, Object>) job.get("conf");
            System.out.println("Conf ID: " + conf.get("id"));
            System.out.println("Date Start: " + conf.get("date_start"));
            System.out.println("Date End: " + conf.get("date_end"));
            System.out.println("Time: " + conf.get("time"));
        }

//        for (int i = 0 ; i<jobs.size() ; i ++ ){
//            System.out.println();
//        }


//        Properties props = new Properties();
//        props.put("bootstrap.servers", bootstrapServers);
//        AdminClient adminClient = AdminClient.create(props);
//
//        DescribeTopicsResult result = adminClient.describeTopics(Collections.singleton(topic),
//                new DescribeTopicsOptions().timeoutMs(50000)); // Adjust timeout as needed
//
//        KafkaFuture<Map<String, TopicDescription>> topicDescriptionFuture = result.all();
//        Map<String, TopicDescription> topicDescriptionMap = topicDescriptionFuture.get();
//
//        TopicDescription topicDescription = topicDescriptionMap.get("postgress");
//        System.out.println("Topic Name: " + topicDescription.name());
//        System.out.println("Partitions: " + topicDescription.partitions().size());
//        System.out.println("Replication Factor: " + topicDescription.partitions().get(0).replicas().size());
//
//        adminClient.close();

//        SparkConf conf = new SparkConf()
//                .setAppName("logs")
//                .setMaster("local[*]");
//
//
//        JavaSparkContext sc = new JavaSparkContext(conf);
//
//
//
//
//
//        Consumer consumer1 = new Consumer(topic, bootstrapServers, "group1"); // Assign a unique group ID
//        new Thread(() -> consumer1.start(sc, null)).start();
//
//        Consumer consumer2 = new Consumer(topic, bootstrapServers, "group2"); // Assign a unique group ID
//        new Thread(() -> consumer2.start(sc, null)).start();
//




//        System.setProperty("hadoop.home.dir", "C:\\spark-3.5.1-bin-hadoop3");
//
//        SparkSession spark = SparkSession
//                .builder()
//                .appName("KafkaStreamReader")
//                .master("local[*]")
//                .getOrCreate();
//
//        // Kafka server configuration
//
//        // Read data from Kafka
//        Dataset<Row> df = spark
//                .readStream()
//                .format("kafka")
//                .option("kafka.bootstrap.servers", bootstrapServers)
//                .option("subscribe", topic)
//                .load();
//
//        // Print data from Kafka stream
//        StreamingQuery query = null;
//        try {
//            query = df.writeStream()
//                    .outputMode("append")
//                    .trigger(Trigger.ProcessingTime("5 seconds"))
//                    .foreach(new ForeachWriter<Row>() {
//                        @Override
//                        public boolean open(long partitionId, long version) {
//                            // Open connection
//                            return true;
//                        }
//
//                        @Override
//                        public void process(Row value) {
//                            // Process each row (print in this case)
//                            System.out.println(value);
//                        }
//
//                        @Override
//                        public void close(Throwable errorOrNull) {
//                            // Close connection
//                        }
//                    })
//                    .start();
//        } catch (TimeoutException e) {
//            throw new RuntimeException(e);
//        }
//
//        try {
//            query.awaitTermination();
//        } catch (StreamingQueryException e) {
//            throw new RuntimeException(e);
//        }
//        SparkConf conf = new SparkConf()
//                .setAppName("logs")
//                .setMaster("local[*]");
//        System.out.println("Current system time when job starts 3: " + new Date());
//
//
//        try {
//            JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(2));
//            System.out.println("JavaStreamingContext initialized successfully");
//
//            Map<String, Object> kafkaParams = new HashMap<>();
//
//            kafkaParams.put("bootstrap.servers", bootstrapServers);
//            kafkaParams.put("key.deserializer", StringDeserializer.class.getName());
//            kafkaParams.put("value.deserializer", StringDeserializer.class.getName());
//            kafkaParams.put("group.id", "group1");
//            kafkaParams.put("enable.auto.commit","true");
//            kafkaParams.put("auto.commit.interval.ms","1000");
//            kafkaParams.put("auto.offset.reset", "latest");
//            kafkaParams.put("client.id","basic-producer-v0.1.0");
//
//            Collection<String> topics = Arrays.asList(topic);
//
//            JavaInputDStream<ConsumerRecord<String, String>> stream =
//                    KafkaUtils.createDirectStream(
//                            streamingContext,
//                            LocationStrategies.PreferConsistent(),
//                            ConsumerStrategies.Subscribe(topics, kafkaParams)
//                    );
//
//
//            stream.foreachRDD(rdd -> {
//                rdd.foreach(record -> {
//                    System.out.println("Received message: " + record.value());
//                    // Add your processing logic here
//                });
//            });
//
//            streamingContext.start();
//            streamingContext.awaitTermination();
//
//            System.out.println("Job completed: " + "");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        Data data = new Data();
//        List<Map<String, Object>> jobs = data.get_jobs();
//
//        SparkConf conf = new SparkConf()
//                .setAppName("logs")
//                .setMaster("local[*]");
//
//
//        JavaSparkContext sc = new JavaSparkContext(conf);

//        Cassandra cassandra = new Cassandra(sc , jobs , logEntry);



//        if (jobs != null){
//            Consumer consumer  = new Consumer(topic,bootstrapServers);
//            consumer.start(sc , null);
//        }
//







    }





}