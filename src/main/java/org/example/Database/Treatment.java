package org.example.Database;

import org.apache.spark.api.java.JavaRDD;
import org.example.Job;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class Treatment {

    String filePath = "output.txt";

    private JavaRDD<Tuple2<String, String>> rdd_log;
    private Map<String, Object> job;

    public Treatment(JavaRDD<Tuple2<String, String>> rdd_log, Map<String, Object> job) {
        this.rdd_log = rdd_log;
        this.job = job;


    }

    public void start(Database database  ) {
        System.out.println("Treatment is starting");
        if (rdd_log == null) {
            System.out.println("RDD log is not initialized.");
            return;
        }
        String body = "";
        System.out.println("RDD log size: " + rdd_log.count());

        String query = rdd_log.filter(tuple -> "QueryEvent".equals(tuple._1())).map(Tuple2::_2).first();
        if (query == null || query.isEmpty()) {
            System.out.println("Query is null or empty.");
            return;
        }


        JavaRDD<Tuple2<String, String>> tableInfo = database.extractTableName(query);

        String table_name_extract = tableInfo.filter(tuple -> "table_name_extract".equals(tuple._1())).map(Tuple2::_2).first();
        String table_alias_extract = "";
        JavaRDD<Object> a  = tableInfo
                .filter(tuple -> "table_alias_extract".equals(tuple._1()))
                .map(Tuple2::_2);
        if (!a.isEmpty()){
            System.out.println("azfa");
            table_alias_extract = tableInfo.filter(tuple -> "table_alias_extract".equals(tuple._1())).map(Tuple2::_2).first();

        }

        String database_name_log = rdd_log.filter(tuple -> "Database".equals(tuple._1())).map(Tuple2::_2).first();

        List<Map<String, Object>> target_databases = (List<Map<String, Object>>) job.get("databases");
        for (Map<String, Object> target_database : target_databases) {
            if (target_database.get("database_name").equals(database_name_log)){
                List<Map<String, Object>> target_tables = (List<Map<String, Object>>) target_database.get("tables");
                for (Map<String, Object> target_table : target_tables) {
                    if (target_table.get("table_name").equals(table_name_extract)){
                        StringBuilder message = new StringBuilder();
                        JavaRDD<String> cols_query = database.extractColumns(query);
                        String tmp_col = "";
                        List<Map<String, Object>> target_columns = (List<Map<String, Object>>) target_table.get("columns");

                        for (String col : cols_query.collect()) {
                            for (Map<String, Object> target_column : target_columns) {
                                tmp_col = (!table_alias_extract.equals("")) ? table_alias_extract + "." + target_column.get("column_name") : (String) target_column.get("column_name");
                                if (tmp_col.equals(col)) {
                                    message.append(target_column.get("column_name")).append(",");
                                }
                            }
                        }

                        if (message.length() > 0) {
                            body =   "database name : " + database_name_log + "\ntable : " + table_name_extract + "\ncolumns : [" + message + "]\n" + body;

                        }
                    }
                }

                }

        }

//            String database_name = String.valueOf(job.getDatabase_name());
//            String table_name = String.valueOf(job.getTable_name());
//            System.out.println("database = " + database_name+ " === " + database_name_log + " table = " + table_name+ " === " + table_name_extract);

//            if (database_name.equals(database_name_log) && table_name.equals(table_name_extract)) {
//                System.out.println("111111111");
//
//
//                StringBuilder message = new StringBuilder();
//                JavaRDD<String> cols_query = database.extractColumns(query);
//                String tmp_col = "";
//                for (String col : cols_query.collect()) {
//                    for (String col_user : job.getCols()) {
//                        tmp_col = (!table_alias_extract.equals("")) ? table_alias_extract + "." + col_user : col_user;
//                        if (tmp_col.equals(col)) {
//                            message.append(col_user).append(",");
//                        }
//                    }
//                }
//
//                if (message.length() > 0) {
//                    body =   "database name : " + database_name + "\ntable : " + table_name + "\ncolumns : [" + message + "]\n" + body;
//
//                }
//            }



        if (body.length() > 0) {

            String client_address = rdd_log.filter(tuple -> "ClientAddress".equals(tuple._1())).map(Tuple2::_2).first();
            body =  "User with IP address :" +client_address +" accessed confidential information in database \n" + body;
            System.out.println(body);
            writeToFile(filePath, body);

//            SendEmail sendEmail = new SendEmail("Alert: Confidential Information Accessed", body);
//            sendEmail.send();
        }








    }

    public static void writeToFile(String filePath, String content) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath , true))) {
            writer.write(content);
            writer.newLine();
        } catch (IOException e) {
            e.printStackTrace();
            // Handle the exception as per your requirement
        }
    }
}
