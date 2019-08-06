package com.javadeveloperzone.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;

import java.util.Arrays;

public class UnionExample {

    public static void main(String[] args){

        SparkConf sparkConf = new SparkConf();

        sparkConf.setMaster("local[1]");

        sparkConf.setAppName("Union Example");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> likes = javaSparkContext.parallelize(Arrays.asList("Java"));

        JavaRDD<String> learn = javaSparkContext.parallelize(Arrays.asList("Spark","Scala"));

        JavaRDD<String> likeToLearn = likes.union(learn);

        System.out.println("I like "+likeToLearn.collect().toString());

        javaSparkContext.stop();

        javaSparkContext.close();

    }
}
