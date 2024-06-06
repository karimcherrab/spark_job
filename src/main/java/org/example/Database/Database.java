package org.example.Database;

import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

public abstract class   Database {


      public abstract JavaRDD<Tuple2<String, String>> extractTableName(String sqlQuery);
      public abstract JavaRDD<String> extractColumns(String sqlQuery);


}
