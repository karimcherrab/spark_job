package org.example.Database.Cassandra;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.example.Database.Database;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CassandraInsertLog  extends Database{

    JavaSparkContext sc;

    public CassandraInsertLog(JavaSparkContext sc) {

        this.sc = sc;

    }




    @Override
    public JavaRDD<Tuple2<String, String>> extractTableName(String sqlQuery) {



        List<Tuple2<String, String>> result = new ArrayList<>();
        String regex = "INSERT INTO [^\\.]+\\.([^ ]+) ";

        Pattern r = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        Matcher m = r.matcher(sqlQuery);

        if (m.find()) {
            result = List.of(
                    new Tuple2<>("table_name_extract", m.group(1))
            );

        }



        return sc.parallelize(result);
    }

    @Override
    public JavaRDD<String> extractColumns(String sqlQuery) {
        ArrayList<String> columnList = new ArrayList<>();

            try {
                // Extract JSON string from the log entry
                int startIndex = sqlQuery.indexOf("JSON{") + 4;
                int endIndex = sqlQuery.lastIndexOf("}");
                String jsonString = sqlQuery.substring(startIndex, endIndex + 1);

                // Split the JSON string by comma to get individual key-value pairs
                String[] keyValuePairs = jsonString.split(",");

                // Extract field names from key-value pairs
                for (String pair : keyValuePairs) {
                    String[] keyValue = pair.split(":");
                    String fieldName = keyValue[0].trim();
                    // Remove leading and trailing whitespaces and quotes if present
                    fieldName = fieldName.replaceAll("^\"|\"$", "");
                    columnList.add(fieldName);
                }
            } catch (Exception e) {
                e.printStackTrace();
                // Handle exceptions, e.g., log or throw error
            }

        return  sc.parallelize(columnList);

    }


}
