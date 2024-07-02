package org.example.Databases.PostgresSQL;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.example.Databases.Database;
import org.example.Databases.DatabaseType;
import org.example.Databases.SparkInstant;
import org.example.Databases.Treatment;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class PostgresSQL  implements DatabaseType, Serializable{
    transient JavaSparkContext sc;
    private Map<String, Object> job;
    private String log;
    private int[] index = {2, 4, 7, 13};
    private JavaRDD<Tuple2<String, String>> rdd_log = null;

    public PostgresSQL(Map<String, Object> job) {
        this.job = job;
         sc = SparkInstant.getInstant_spark();

    }


    @Override
    public JavaRDD<Tuple2<String, String>> convertLogToRDD(int[] index, String log) {
        try {



            String[] parts = log.split(";");
            if (parts.length >= 14) {
                String database_name = parts[index[0]].replaceAll("\"", "");
                String client_address = parts[index[1]].replaceAll("\"", "");
                String command_tag = parts[index[2]].replaceAll("\"", "");
                String query_event = parts[index[3]].replaceAll("\"", "");
                System.out.println(query_event);
                List<Tuple2<String, String>> keyValuePairs = Arrays.asList(
                        new Tuple2<>("Database", database_name),
                        new Tuple2<>("ClientAddress", client_address),
                        new Tuple2<>("CommandTag", command_tag),
                        new Tuple2<>("QueryEvent", query_event)
                );

                JavaRDD<Tuple2<String, String>> rdd = sc.parallelize(keyValuePairs);
                this.rdd_log = rdd;

                typeLog(command_tag);
                return rdd_log;
            } else {
                System.out.println("Not enough elements in the array.");
                return null;
            }
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
                        System.out.println("return");
                        return;
                    }

                }
            }

        }



        if ("SELECT".equals(commandTag)) {
            System.out.println("SELECT");
            database = new PostgresSelectLog(sc);
            Treatment treatment = new Treatment(rdd_log, job);
            treatment.start(database);
        } else if ("INSERT".equals(commandTag)) {
            System.out.println("INSERT");
            database = new PostgresInsertLog(sc);
            Treatment treatment = new Treatment(rdd_log, job);
            treatment.start(database);
        }




    }




//    @Override
//    public JavaRDD<Tuple2<String, String>> convertLogToRDD(int[] index, String log) {
//        try {
//            String[] parts = log.split(",");
//
//            if (parts.length >= 14) {
//                String database_name = parts[index[0]].replaceAll("\"", "");
//                String client_address = parts[index[1]].replaceAll("\"", "");
//                String command_tag = parts[index[2]].replaceAll("\"", "");
//                String query_event = "";
//
//                Pattern pattern = Pattern.compile("execute.*?PostgreSQL JDBC Driver");
//                Matcher matcher = pattern.matcher(log);
//
//                if (matcher.find()) {
//                    query_event = matcher.group();
//                }
//
//                List<Tuple2<String, String>> keyValuePairs = Arrays.asList(
//                        new Tuple2<>("Database", database_name),
//                        new Tuple2<>("ClientAddress", client_address),
//                        new Tuple2<>("CommandTag", command_tag),
//                        new Tuple2<>("QueryEvent", query_event)
//                );
//
//                if (rdd_log == null) {  // Lazy initialization
//                    JavaRDD<Tuple2<String, String>> rdd = sc.parallelize(keyValuePairs);
//                    this.rdd_log = rdd;
//                }
//
//                typeLog(command_tag);
//
//                return rdd_log;
//            } else {
//                System.out.println("Not enough elements in the array.");
//                return null;
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//            return null;
//        }
//    }

}