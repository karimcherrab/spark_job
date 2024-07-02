package org.example.Databases.Cassandra;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.example.Databases.Database;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CassandraSelectLog extends Database {
    JavaSparkContext sc;
    public CassandraSelectLog(JavaSparkContext sc) {

        this.sc = sc;


    }


    @Override
    public JavaRDD<Tuple2<String, String>> extractTableName(String sqlQuery) {
        List<Tuple2<String, String>> result = new ArrayList<>();

        // Regular expression patterns to match table names with optional alias
        String pattern1 = "\\b(?:FROM|JOIN)\\s+([\\w.]+)(?:\\s+AS\\s+(\\w+))?\\s*(?=WHERE|JOIN|$)";
        String pattern2 = "SELECT \\* FROM ([\\w_]+)(?: AS ([\\w_]+))?";

        // Compile and match pattern1
        Pattern r1 = Pattern.compile(pattern1, Pattern.CASE_INSENSITIVE);
        Matcher m1 = r1.matcher(sqlQuery);

        if (m1.find()) {
            String tableName = m1.group(1);
            String tableAlias = m1.group(2) != null ? m1.group(2) : "";

            result.add(new Tuple2<>("table_name_extract", tableName));
            result.add(new Tuple2<>("table_alias_extract", tableAlias));


        } else {
            // Compile and match pattern2
            Pattern r2 = Pattern.compile(pattern2, Pattern.CASE_INSENSITIVE);
            Matcher m2 = r2.matcher(sqlQuery);

            if (m2.find()) {
                String tableName = m2.group(1);
                String tableAlias = m2.group(2) != null ? m2.group(2) : "";

                result.add(new Tuple2<>("table_name_extract", tableName));
                result.add(new Tuple2<>("table_alias_extract", tableAlias));


            }
        }

        return sc.parallelize(result);
    }


    public JavaRDD<String> extractColumns(String sqlQuery) {
        // Extract column names from SELECT clause
        List<String> columns = new ArrayList<>();

        // Extract column names from SELECT clause
        String selectRegex = "(?i)select\\s+(.*?)\\s+from";
        Pattern selectPattern = Pattern.compile(selectRegex);
        Matcher selectMatcher = selectPattern.matcher(sqlQuery);

        if (selectMatcher.find()) {
            String columnsPart = selectMatcher.group(1);
            columns.addAll(Arrays.asList(columnsPart.split("\\s*,\\s*")));
        } else {
            throw new IllegalArgumentException("Column names not found in SELECT clause of query: " + sqlQuery);
        }

        // Extract column names from WHERE clause
        String whereRegex = "(?i)where\\s+(.*)";
        Pattern wherePattern = Pattern.compile(whereRegex);
        Matcher whereMatcher = wherePattern.matcher(sqlQuery);

        if (whereMatcher.find()) {
            String wherePart = whereMatcher.group(1);
            String[] conditions = wherePart.split("\\s+and\\s+|\\s+or\\s+");
            for (String condition : conditions) {
                String[] conditionParts = condition.split("\\s*[<>=!]+\\s*");
                if (conditionParts.length > 0) {
                    columns.add(conditionParts[0].trim());
                }
            }
        }
//        for (String a : columns){
//            System.out.println(a);
//        }

        return sc.parallelize(columns);
    }
}
