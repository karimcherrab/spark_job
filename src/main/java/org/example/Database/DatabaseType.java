package org.example.Database;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public interface DatabaseType {


    JavaRDD<Tuple2<String, String>> convertLogToRDD(int[] index , String log);
    void typeLog(String commandTag);

}
