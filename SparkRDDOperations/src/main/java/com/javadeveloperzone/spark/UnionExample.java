package com.javadeveloperzone.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;

import java.util.Arrays;
import java.util.List;

public class UnionExample {

    private static List<String> ArrayList;

    public static void main(String[] args){

        SparkConf sparkConf = new SparkConf();

        sparkConf.setMaster("local[1]");

        sparkConf.setAppName("Union Example");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> likes = javaSparkContext.parallelize(Arrays.asList("Java"));

        JavaRDD<String> learn = javaSparkContext.parallelize(Arrays.asList("Spark","Scala"));

        JavaRDD<String> likeToLearn = likes.union(learn);

        List<String> result = likeToLearn.collect();

        //Learning::2
        System.out.println("Learning::"+learn.count());

        //Prints I like [Java, Spark, Scala]
        System.out.println("I like "+result.toString());

        String[]  newlyLearningSkills = {"Elastic Search","Spring Boot"};

        JavaRDD<String> learningSkills = learn.union(javaSparkContext.parallelize(Arrays.asList(newlyLearningSkills)));

//        learningSkills::[Spark, Scala, Elastic Search, Spring Boot]
        System.out.print("learningSkills::"+learningSkills.collect().toString());



        javaSparkContext.stop();

        javaSparkContext.close();

    }
}
