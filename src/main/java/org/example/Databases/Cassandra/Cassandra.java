package org.example.Databases.Cassandra;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.example.Databases.Database;
import org.example.Databases.DatabaseType;
import org.example.Databases.SparkInstant;
import org.example.Databases.Treatment;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public  class Cassandra implements DatabaseType {
    transient JavaSparkContext sc ;
    private  Map<String, Object> job;

    private  String log;


    private JavaRDD<Tuple2<String, String>> rdd_log = null;
    public Cassandra(Map<String, Object> job) {
        this.job = job;
        sc = SparkInstant.getInstant_spark();
    }


    @Override
    public JavaRDD<Tuple2<String, String>> convertLogToRDD(int[] index , String log) {
        try {
            String[] parts = log.split(";");


//                String client_address = parts[index[1]].replaceAll("\"", "");

                String client_address = parts[index[0]].replaceAll("\"", "");
                String query_event = parts[index[1]].replaceAll("\"", "");

                 String command_tag = extract_comment_teg(query_event);
                 String database_name = extractDatabaseName(query_event);
                List<Tuple2<String, String>> keyValuePairs = Arrays.asList(
                        new Tuple2<>("Database", database_name),
                        new Tuple2<>("ClientAddress", client_address),
                        new Tuple2<>("CommandTag", command_tag),
                        new Tuple2<>("QueryEvent", query_event)
                );
//
                JavaRDD<Tuple2<String, String>> rdd = sc.parallelize(keyValuePairs);

                this.rdd_log = rdd;

                typeLog(command_tag);

                return rdd;

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void typeLog(String commandTag) {
        Database database = null;
        String client_address = rdd_log.filter(tuple -> "ClientAddress".equals(tuple._1())).map(Tuple2::_2).first();
        String database_name_log = rdd_log.filter(tuple -> "Database".equals(tuple._1())).map(Tuple2::_2).first();

        List<Map<String, Object>> target_databases = (List<Map<String, Object>>) job.get("databases");
        for (Map<String, Object> target_database : target_databases) {
            if (target_database.get("database_name").equals(database_name_log)){
                List<Map<String, Object>> allowed_Users = (List<Map<String, Object>>) target_database.get("allowedUsers");
                for (Map<String, Object> allowed_User : allowed_Users) {
                    if (allowed_User.get("name").equals(client_address)){
                        return;
                    }

                }
            }

        }

        if ("SELECT".equals(commandTag)) {
//            String database_name_log = rdd_log.filter(tuple -> "Database".equals(tuple._1())).map(Tuple2::_2).first();
//
//            System.out.println(database_name_log);
            database = new CassandraSelectLog(sc);
            Treatment treatment = new Treatment(rdd_log , job);
            treatment.start(database);
        } else if ("INSERT".equals(commandTag)) {
            database = new CassandraInsertLog(sc);
            Treatment treatment = new Treatment(rdd_log , job);
            treatment.start(database);
        } else if ("UPDATE".equals(commandTag)) {
            database = new CassandraUpdateLog(sc);
            Treatment treatment = new Treatment(rdd_log , job);
            treatment.start(database);
        }


    }





    public String extract_comment_teg(String query){
        String patternString = "'(INSERT|SELECT|UPDATE|DELETE)\\b";
        Pattern pattern = Pattern.compile(patternString);
        Matcher matcher = pattern.matcher(query);
        String queryType = "";
        // Search for the pattern in the log entry
        if (matcher.find()) {
             queryType = matcher.group(1);
        }

        return queryType;
    }




    public static String extractDatabaseName(String logLine) {
        String dbName = "";

        // Pattern to extract keyspace name from 'logged keyspace' logs
        String keyspaceRegex = "logged keyspace\\s+'([a-zA-Z0-9_]+)'";
        Pattern keyspacePattern = Pattern.compile(keyspaceRegex, Pattern.CASE_INSENSITIVE);

        // Pattern to extract database name from 'INSERT INTO' logs
        String insertIntoRegex = "INSERT INTO ([^\\.]+)\\.";

        Pattern insertIntoPattern = Pattern.compile(insertIntoRegex, Pattern.CASE_INSENSITIVE);
        Matcher insertIntoMatcher = insertIntoPattern.matcher(logLine);
        if (insertIntoMatcher.find()) {

            dbName = insertIntoMatcher.group(1);

        }else {
            // If keyspace pattern does not match, check against insert into pattern
            Matcher keyspaceMatcher = keyspacePattern.matcher(logLine);
            if (keyspaceMatcher.find()) {
                dbName = keyspaceMatcher.group(1);
            }

        }


        return dbName;
    }
}







