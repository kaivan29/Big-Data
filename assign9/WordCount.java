package com.refactorlabs.cs378.assign9;

import com.google.common.collect.Lists;
//import com.refactorlabs.cs378.utils.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

/**
 * WordCount application for Spark.
 */
public class WordCount {
	public static void main(String[] args) {
		//Utils.printClassPath();

		String inputFilename = args[0];
		String outputFilename = args[1];

		// Create a Java Spark context
		SparkConf conf = new SparkConf().setAppName(WordCount.class.getName()).setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load the input data
		JavaRDD<String> input = sc.textFile(inputFilename);

		// Split the input into words
		FlatMapFunction<String, String> splitFunction =
				new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String line) throws Exception {
				StringTokenizer tokenizer = new StringTokenizer(line);
				List<String> wordList = Lists.newArrayList();

				// For each word in the input line, emit that word.
				while (tokenizer.hasMoreTokens()) {
					wordList.add(tokenizer.nextToken());
				}
				return wordList.iterator();
			}
		};

		// Transform into word and count
		PairFunction<String, String, Integer> addCountFunction =
				new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) throws Exception {
				return new Tuple2(s, 1);
			}
		};

        // Sum the counts
		Function2<Integer, Integer, Integer> sumFunction =
				new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer count1, Integer count2) throws Exception {
				return count1 + count2;
			}
		};

		JavaRDD<String> words = input.flatMap(splitFunction);
		JavaPairRDD<String, Integer> wordsWithCount = words.mapToPair(addCountFunction);
		JavaPairRDD<String, Integer> counts = wordsWithCount.reduceByKey(sumFunction);
        JavaPairRDD<String, Integer> ordered = counts.sortByKey();

        // All in one line:
		// JavaPairRDD<String, Integer> ordered =
        //          input.flatMap(splitFunction).mapToPair(addCountFunction)
		//               .reduceByKey(sumFunction).sortByKey();

		// Save the word count to a text file (initiates evaluation)
		ordered.saveAsTextFile(outputFilename);

		// Shut down the context
		sc.stop();
	}

}
