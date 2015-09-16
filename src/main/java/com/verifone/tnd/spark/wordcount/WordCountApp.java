package com.verifone.tnd.spark.wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Spark Training - Word Count Application.
 *
 * @author Yohan Liyanage
 */
public class WordCountApp {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Word-Count");
        conf.setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // Read all lines from the text file
        JavaRDD<String> lines = sc.textFile("sample.txt");

        // Split each line to words by whitespace
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split("\\s+")));

        // Create a tuple for each word, with initial count of word being 1
        JavaPairRDD<String, Integer> wordTuples = words.mapToPair(word -> new Tuple2<>(word, 1));

        // Aggregation Step - Reduce by Word
        JavaPairRDD<String, Integer> wordCounts = wordTuples.reduceByKey((count1, count2) -> count1 + count2);

        // Collect the final word counts
        List<Tuple2<String, Integer>> wordCountList = wordCounts.collect();

        // Print each word count result
        wordCountList.forEach(tuple -> System.out.println(tuple._1() + " : " + tuple._2()));
    }
}
