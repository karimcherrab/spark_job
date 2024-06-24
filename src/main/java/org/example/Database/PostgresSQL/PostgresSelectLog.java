package org.example.Database.PostgresSQL;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.example.Database.Database;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PostgresSelectLog extends Database {

    JavaSparkContext sc;

    public PostgresSelectLog(JavaSparkContext sc) {
        super();
        this.sc = sc;

    }

    @Override
    public JavaRDD<Tuple2<String, String>> extractTableName(String sqlQuery) {
        List<Tuple2<String, String>> result = new ArrayList<>();
        // Regular expression pattern to match table names with alias
        String pattern = "\\b(?:FROM|JOIN)\\s+([\\w.]+)\\s+(?:AS\\s+)?(\\w+)?";
        Pattern r = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
        Matcher m = r.matcher(sqlQuery);

        if (m.find()) {

            result = Arrays.asList(
                    new Tuple2<>("table_name_extract", m.group(1)),
                    new Tuple2<>("table_alias_extract", m.group(2))
            );
        }


        return sc.parallelize(result);
    }


    @Override
    public JavaRDD<String> extractColumns(String sqlQuery) {

        Pattern columnPattern = Pattern.compile("select\\s+(.+?)\\s+from", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher columnMatcher = columnPattern.matcher(sqlQuery);
        String columnString = "";
        if (columnMatcher.find()) {
            columnString = columnMatcher.group(1);
        } else {
            System.out.println("Column names not found in the query.");

        }

        // Splitting column names
        String[] columns = columnString.split(",");
        for (int i = 0; i < columns.length; i++) {
            columns[i] = columns[i].substring(0 , columns[i].indexOf(" as "));
            columns[i] = columns[i].trim();
        }
        return  sc.parallelize(Arrays.asList(columns));

    }

//    @Override
//    public void treatment() {
//        String body= "";
//        String query = rdd_log.filter(tuple -> tuple._1().equals("QueryEvent")).first()._2();
//
//        JavaRDD<Tuple2<String, String>> tableInfo = extractTableName(query);
//
//        String table_name_extract = tableInfo.filter(tuple -> tuple._1().equals("table_name_extract")).first()._2();
//        String table_alias_extract = tableInfo.filter(tuple -> tuple._1().equals("table_alias_extract")).first()._2();
//
//        String database_name_log = rdd_log.filter(tuple -> tuple._1().equals("Database")).first()._2();
//        for (Map<String, Object> job : jobs) {
//
//            String database_name = String.valueOf(job.get("database_name"));
//            String table_name = String.valueOf(job.get("table_name"));
//            if (database_name.equals(database_name_log) && table_name.equals(table_name_extract)){
//                List<String> columnNames = (List<String>) job.get("column_names");
//
//                String message = "";
//                ArrayList<String> cols_query = extractColumns(query);
//                String tmp_col = "";
//                for (String col : cols_query){
//                    for (String col_user : columnNames){
//
//                        tmp_col = table_alias_extract +"."+ col_user;
//
//                        if(tmp_col.equals(col)){
//                            message = col_user + ","+message;
//                        }
//                    }
//
//
//
//                }
//
//
//
//                if(!message.equals("")){
//                    String client_address = rdd_log.filter(tuple -> tuple._1().equals("ClientAddress")).first()._2();
//
//                    body = "User with IP address"+client_address+ " accessed confidential information in database "+database_name+", table "+table_name+", columns ["+message+"]\n"  ;
//
//                    SendEmail sendEmail = new SendEmail("Alert: Confidential Information Accessed" , body);
//                    sendEmail.send();
//
//                }else {
//                    System.out.println("Not found " );
//
//                }
//            }
//
//        }
//    }
}
