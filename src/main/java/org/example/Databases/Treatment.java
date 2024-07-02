package org.example.Databases;

import org.apache.spark.api.java.JavaRDD;
import org.example.Data;
import scala.Tuple2;

import java.util.ArrayList;
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
        TargetData targetData  = null;
        try {
            if (rdd_log == null) {
                System.out.println("RDD log is not initialized.");
                return;
            }
            String body = "";

            String query = rdd_log.filter(tuple -> "QueryEvent".equals(tuple._1())).map(Tuple2::_2).first();
            if (query == null || query.isEmpty()) {
                System.out.println("Query is null or empty.");
                return;
            }

            JavaRDD<Tuple2<String, String>> tableInfo = database.extractTableName(query);

            String table_name_extract = tableInfo.filter(tuple -> "table_name_extract".equals(tuple._1())).map(Tuple2::_2).first();
            String table_alias_extract = "";
            JavaRDD<Object> table_alias_extract_rdd  = tableInfo
                    .filter(tuple -> "table_alias_extract".equals(tuple._1()))
                    .map(Tuple2::_2);
            if (!table_alias_extract_rdd.isEmpty()){
                table_alias_extract = tableInfo.filter(tuple -> "table_alias_extract".equals(tuple._1())).map(Tuple2::_2).first();

            }

            String database_name_log = rdd_log.filter(tuple -> "Database".equals(tuple._1())).map(Tuple2::_2).first();
            List<Map<String, Object>> target_databases = (List<Map<String, Object>>) job.get("databases");
            for (Map<String, Object> target_database : target_databases) {

                if (target_database.get("database_name").equals(database_name_log)){
                System.out.println("check table");
                    List<Map<String, Object>> target_tables = (List<Map<String, Object>>) target_database.get("tables");

                    for (Map<String, Object> target_table : target_tables) {
                        System.out.println(target_table.get("table_name") + "/ Vs /" + table_name_extract );

                        if (target_table.get("table_name").equals(table_name_extract)){

                            System.out.println("check cols");

                            ArrayList<String> list_cols_match = new ArrayList();

                            JavaRDD<String> cols_query = database.extractColumns(query);

                            String tmp_col = "";
                            List<Map<String, Object>> target_columns = (List<Map<String, Object>>) target_table.get("columns");

                            for (String col : cols_query.collect()) {

                                if (col.equals("*")){
                                    for (Map<String, Object> target_column : target_columns) {

                                            list_cols_match.add((String) target_column.get("column_name"));

                                    }
                                }else {
                                    for (Map<String, Object> target_column : target_columns) {
                                        System.out.println(col + "/ Vs /" + target_column.get("column_name") );

                                        tmp_col = (!table_alias_extract.equals("")) ? table_alias_extract + "." + target_column.get("column_name") : (String) target_column.get("column_name");
                                        if (tmp_col.equals(col)) {

                                            list_cols_match.add((String) target_column.get("column_name"));
                                        }
                                    }
                                }
                            }

                            if (list_cols_match.size() > 0) {
//                                body =   "database name : " + database_name_log + "\ntable : " + table_name_extract + "\ncolumns : [" + message + "]\n" + body;
                                targetData = new TargetData(database_name_log , table_name_extract , list_cols_match);
                            }




                        }
                    }
                }
            }


            if (targetData != null) {

                String client_address = rdd_log.filter(tuple -> "ClientAddress".equals(tuple._1())).map(Tuple2::_2).first();
                body =  "User with IP address :" +client_address +" accessed confidential information in database \n" + body;
                System.out.println(body);
                Data data = new Data();
                List<String> list_tables  = new ArrayList<>();
                list_tables.add(targetData.table_name);
                String response = data.postAlert((int) job.get("id"),(String) job.get("type"), client_address, targetData.database_name , list_tables ,  targetData.list_columns);
                System.out.println("Response from server: " + response);
//                String file_name = (String) job.get("name") +".csv";
//                writeToFile(file_name,client_address ,  targetData );
//
//                SendEmail sendEmail = new SendEmail("Alert: Confidential Information Accessed", body);
//                sendEmail.send();
            }

        }catch (Exception exception){
            System.out.println("Error : "  + exception.getMessage() );
        }







    }
//    CsvAppender csvAppender = new CsvAppender();
//
//    public static void writeToFile(String filePath, String client_address , TargetData targetData) {
//        String[] header = {"DatabaseType", "ClientAddress","Database","Tables","columns","create_at"};
//
//        String cols = String.join(", ", targetData.list_columns);
//        String[] data = {"cassandra", client_address  ,targetData.database_name , targetData.table_name , cols };
//        CsvAppender.appendToCsv(filePath, header, data);
//
//        System.out.println("Data appended to CSV file successfully.");
////        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath , true))) {
////            writer.write(content);
////            writer.newLine();
////        } catch (IOException e) {
////            e.printStackTrace();
////            // Handle the exception as per your requirement
////        }
//    }
}
