package org.example.Databases;

import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

public interface DatabaseType {


    JavaRDD<Tuple2<String, String>> convertLogToRDD(int[] index , String log);
    void typeLog(String commandTag);

}
