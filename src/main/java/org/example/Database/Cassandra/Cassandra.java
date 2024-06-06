package org.example.Database.Cassandra;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.example.Database.Database;
import org.example.Database.DatabaseType;
import org.example.Database.PostgresSQL.PostgresInsertLog;
import org.example.Database.PostgresSQL.PostgresSelectLog;
import org.example.Database.Treatment;
import org.example.Job;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public  class Cassandra implements DatabaseType {
    JavaSparkContext sc ;
    Map<String, Object> job;

    String log;

    int[] index = {5};

    JavaRDD<Tuple2<String, String>> rdd_log  ;
    public Cassandra(JavaSparkContext sc,Map<String, Object> job, String log) {
        this.sc = sc;
        this.job = job;
        this.log = log;
        rdd_log = convertLogToRDD(index , log);
    }


    @Override
    public JavaRDD<Tuple2<String, String>> convertLogToRDD(int[] index , String log) {
        try {
            String[] parts = log.split(";");


//                String client_address = parts[index[1]].replaceAll("\"", "");

                String query_event = parts[index[0]].replaceAll("\"", "");
                System.out.println(query_event);

                 String command_tag = extract_comment_teg(query_event);
            String database_name = extractDatabaseName(query_event);

                List<Tuple2<String, String>> keyValuePairs = Arrays.asList(
                        new Tuple2<>("Database", database_name),
                        new Tuple2<>("ClientAddress", "localhost"),
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

        if ("SELECT".equals(commandTag)) {
            System.out.println("SelectLog");
            database = new CassandraSelectLog(sc);
        } else if ("INSERT".equals(commandTag)) {
            database = new CassandraInsertLog(sc);
        } else if ("UPDATE".equals(commandTag)) {
            database = new CassandraUpdateLog(sc);
        }

        if (database != null) {
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
        System.out.println(queryType);

        return queryType;
    }


    public static String extractDatabaseName(String logLine) {
        String dbName = "";
        Pattern dbPattern = Pattern.compile("INSERT INTO ([^\\.]+)\\.");
        Matcher dbMatcher = dbPattern.matcher(logLine);
        if (dbMatcher.find()) {
            dbName = dbMatcher.group(1);
        }
        System.out.println("database = " + dbName);
        return "data1";
    }

}
