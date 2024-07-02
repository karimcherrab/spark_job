package org.example.Databases;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkInstant {

    public static JavaSparkContext sc = null;
    public static SparkConf conf = null;

    public SparkInstant() {
        // private constructor to prevent instantiation
    }

    public static JavaSparkContext getInstant_spark() {
        if (sc == null) {
            synchronized (SparkInstant.class) {
                if (sc == null) {
                    conf = new SparkConf()
                            .setAppName("logs")
                            .setMaster("local[*]");
                    sc = new JavaSparkContext(conf);
                }
            }
        }
        return sc;
    }

    public static void stopSparkContext() {
        if (sc != null) {
            sc.close();
            sc = null;
        }
    }
}
