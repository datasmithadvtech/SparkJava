package com.javadeveloperzone.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ReduceByKeyExample {

    public static void main(String[] args){

        SparkConf sparkConf = new SparkConf();

        sparkConf.setMaster("local[1]");

        sparkConf.setAppName(" Spark Reduce By Key Example");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        List<Integer> range = IntStream.rangeClosed(1, 20).boxed().collect(Collectors.toList());

        JavaRDD<Integer> intRange = javaSparkContext.parallelize(range,4);

        JavaPairRDD<String,Integer> intPairedRDD= intRange.mapToPair(new PairFunction<Integer, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Integer value) throws Exception {
                Tuple2<String, Integer> tuple2 = new Tuple2<>("key",value);
                return tuple2;
            }
        });

        JavaPairRDD<String,Integer> result = intPairedRDD.reduceByKey((v1, v2) -> (v1+v2));

        List<Tuple2<String,Integer>> collectedResult=result.collect();

        //Prints  collected result
        System.out.print(collectedResult);

        //Iterating through all the elements, i.e here we do have only 1 element
        //below code is for understanding purpose.
        for(Tuple2<String,Integer> element : collectedResult){
            System.out.println("key::"+element._1()+" val::"+element._2());
        }

        javaSparkContext.stop();

        javaSparkContext.close();

    }
}
