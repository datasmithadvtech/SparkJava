package com.javadeveloperzone.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class ReduceExample {

    public static void main(String[] args){

        SparkConf sparkConf = new SparkConf();

        sparkConf.setAppName("Spark Reduce Example using Java");

        //Setting Master for running it from IDE.
        sparkConf.setMaster("local[2]");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<Integer> intRdd =  javaSparkContext.parallelize(Arrays.asList(1,2,3,4,5,6));

        Integer result = intRdd.reduce((v1, v2) -> v1+v2);

        System.out.println("result::"+result);

        Integer foldResult = intRdd.fold(0,((v1, v2) -> (v1+v2)));

        System.out.println("Fold result::"+foldResult);

    }
}
