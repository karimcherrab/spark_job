package org.example.Database.Cassandra;


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
public class CassandraUpdateLog extends Database {
    JavaSparkContext sc;
    public CassandraUpdateLog(JavaSparkContext sc) {

        this.sc = sc;


    }
    public JavaRDD<Tuple2<String, String>> extractTableName(String sqlQuery) {
        List<Tuple2<String, String>> result = new ArrayList<>();
        // Regular expression pattern to match the table name in the UPDATE statement
        String pattern = "UPDATE\\s+\\w+\\.(\\w+)";
        Pattern r = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
        Matcher m = r.matcher(sqlQuery);

        if (m.find()) {
            String tableName = m.group(1);
            result.add(new Tuple2<>("table_name_extract", tableName));
        }

        return sc.parallelize(result);
    }

    @Override
    public JavaRDD<String> extractColumns(String sqlQuery) {
        List<String> columns = new ArrayList<>();

        // Regular expression patterns to match column names in SET and WHERE clauses
        String setClausePattern = "SET\\s+([^;]+?)\\s+WHERE";
        String whereClausePattern = "WHERE\\s+([^;]+)";

        Pattern setPattern = Pattern.compile(setClausePattern, Pattern.CASE_INSENSITIVE);
        Matcher setMatcher = setPattern.matcher(sqlQuery);

        if (setMatcher.find()) {
            String setClause = setMatcher.group(1);
            String[] setColumns = setClause.split("\\s*,\\s*(?![^\\[]*\\])"); // Split by comma not within brackets
            for (String col : setColumns) {
                String columnName = col.split("\\s*=\\s*")[0].trim();
                columns.add(columnName);
            }
        }

        Pattern wherePattern = Pattern.compile(whereClausePattern, Pattern.CASE_INSENSITIVE);
        Matcher whereMatcher = wherePattern.matcher(sqlQuery);

        if (whereMatcher.find()) {
            String whereClause = whereMatcher.group(1);
            String[] whereColumns = whereClause.split("\\s+and\\s+");
            for (String col : whereColumns) {
                String columnName = col.split("\\s*=\\s*")[0].trim();
                columns.add(columnName);
            }
        }

//        for (String a : columns){
//            System.out.println(a);
//        }

        return sc.parallelize(columns);
    }



}
