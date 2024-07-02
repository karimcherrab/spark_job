package org.example.Databases.PostgresSQL;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.example.Databases.Database;
import scala.Tuple2;

import java.util.ArrayList;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PostgresInsertLog  extends Database {
    JavaSparkContext sc;
    public PostgresInsertLog(JavaSparkContext sc) {

        this.sc = sc;


    }

    @Override
    public JavaRDD<Tuple2<String, String>> extractTableName(String sqlQuery) {
        List<Tuple2<String, String>> result = new ArrayList<>();
        String regex = "INSERT\\s+INTO\\s+([a-zA-Z0-9_]+)";
        Pattern r = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        Matcher m = r.matcher(sqlQuery);

        if (m.find()) {
            result = List.of(
                    new Tuple2<>("table_name_extract", m.group(1))
            );
        } else {
            System.out.println("Failed to extractTableName");
        }

        return sc.parallelize(result);
    }

    @Override
    public JavaRDD<String> extractColumns(String sqlQuery) {
        ArrayList<String> columnList = new ArrayList<>();

        // Regular expression to match the columns inside parentheses after "INSERT INTO" statement
        String regex = "INSERT\\s+INTO\\s+[^\\(]+\\(([^\\)]+)\\)";

        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(sqlQuery);

        if (matcher.find()) {
            String columnsStr = matcher.group(1);

            String[] columnsArray = columnsStr.split(",");

            for (String column : columnsArray) {
                System.out.println("col name : "  +column );
                columnList.add(column.trim().replace("\"\"" , ""));
            }
        }

        return  sc.parallelize(columnList);
    }


}
