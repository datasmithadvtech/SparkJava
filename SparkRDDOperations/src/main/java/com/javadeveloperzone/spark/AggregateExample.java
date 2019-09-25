package com.javadeveloperzone.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.io.Serializable;
import java.util.Arrays;

public class AggregateExample {

    public static void main(String[] args ){

        SparkConf sparkConf = new SparkConf();

        sparkConf.setAppName("Spark Aggregate Count Example");

        sparkConf.setMaster("local[1]");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<Integer> intRDD = javaSparkContext.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9,10));

        Function2<AverageCount, Integer, AverageCount> addAndCount =
                new Function2<AverageCount, Integer, AverageCount>() {
                    public AverageCount call(AverageCount a, Integer x) {
                        a.total += x;
                        a.num += 1;
                        return a;
                    }
                };

        Function2<AverageCount, AverageCount, AverageCount> combine =
                new Function2<AverageCount, AverageCount, AverageCount>() {
                    public AverageCount call(AverageCount a, AverageCount b) {
                        a.total += b.total;
                        a.num += b.num;
                        return a;
                    }
                };


        AverageCount initial = new AverageCount(0, 0);
        AverageCount currentMovingAverage = intRDD.aggregate(initial, addAndCount, combine);
        System.out.println("Moving Average:"+currentMovingAverage.avg());

        JavaRDD<Integer> anotherIntRDD = javaSparkContext.parallelize(Arrays.asList(11,12,13));

        JavaRDD<Integer> resultantRDD = intRDD.union(anotherIntRDD);

        AverageCount newMovingAverage = resultantRDD.aggregate(initial, addAndCount, combine);

        System.out.println("Changed Moving Average:"+newMovingAverage.avg());


    }
}
