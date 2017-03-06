package com.refactorlabs.cs378.assign10;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import com.google.common.collect.Lists;
import scala.Tuple2;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.io.IOException;
import java.util.*;


public class InvertedIndex {
	public static void main(String[] args) {

		String inputFilename = args[0];
		String outputFilename = args[1];

		// Create a Java Spark context
		SparkConf conf = new SparkConf().setAppName(InvertedIndex.class.getName()).setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load the input data
		JavaRDD<String> input = sc.textFile(inputFilename);

		// Split the input into words
		FlatMapFunction<String, Tuple2<String,String>> splitFunction =
				new FlatMapFunction<String, Tuple2<String,String>>() {
			@Override
			public Iterator<Tuple2<String,String>> call(String line) throws Exception {
				//StringTokenizer tokenizer = new StringTokenizer(line);
				List<String> arr_words = new ArrayList<String>();
				List<Tuple2<String,String>> wordList = new ArrayList<Tuple2<String,String>>();

				if(line == "");
				else {
					line = line.replaceAll("[,.;_&*?!()]", "");
					line = line.replaceAll("[\"]", "");
					line = line.replaceAll("  ", " ");
					String arr[] = line.split(" ");
					String verse = arr[0];

					//Check for the duplicates
					for (int i = 1; i < arr.length; ++i) {
							arr[i] = arr[i].replaceAll(":","");
							arr[i] = arr[i].toLowerCase();
							if (arr_words.contains(arr[i])) ;
							else{
								arr_words.add(arr[i]);
								wordList.add(new Tuple2(arr[i],verse)); //check syntax for this
							}
					}
				}
				return wordList.iterator();
			}
		};

		// Transform into word and n/
		PairFunction<Tuple2<String,String>, String, String> tupleFunction =
				new PairFunction<Tuple2<String,String>, String, String>() {
			@Override
			public Tuple2<String, String> call(Tuple2<String,String> r) throws Exception {
				return r;
			}
		};

		Function2<String, String, String> concatFunction =
				new Function2<String, String, String>() {
			@Override
			public String call(String n1, String n2) throws Exception {
				return n1 +','+ n2;
			}
		};

		Function<String, Iterable<String>> sortFunction =
				new Function<String, Iterable<String>>() {
					@Override
					public Iterable<String> call(String verses) throws Exception {
						// TODO Auto-generated method stub
						String[] verseArray = verses.split(",");
						String[] tempArray = new String[verseArray.length];
						int n = 0;
						
						Arrays.sort(verseArray);
						//Complete Sort
						Arrays.sort(verseArray, new Comparator<String>() {
							public int compare(String o1, String o2){
								String temp1[] = o1.split(":");
								String temp2[] = o2.split(":");
								int compareVal = 0;
								compareVal = temp1[0].compareTo(temp2[0]);
								if (compareVal != 0)
									return compareVal;
								else{
									compareVal = Integer.valueOf(temp1[1]).compareTo(Integer.valueOf(temp2[1]));
									if (compareVal != 0)
										return compareVal;
									else{
										compareVal = Integer.valueOf(temp1[2]).compareTo(Integer.valueOf(temp2[2]));
										return compareVal;
									}
								}
							}
						});
						
						for (String verse : verseArray)
						{
							tempArray[n] = verse;
							n++;
						}
						
						String a = tempArray[0];
						String b = tempArray[tempArray.length - 1];
						tempArray[0] = b;
						tempArray[tempArray.length - 1] = a;
						
						List<String> verseList = Arrays.asList(verseArray);
						String verse  = verseList.toString();
						
						ArrayList<String> finalVerse = new ArrayList<String>();
						finalVerse.add(verse);
						
						return finalVerse;						
					}
		};

		JavaRDD<Tuple2<String,String>> words = input.flatMap(splitFunction);
		JavaPairRDD<String, String> wordsTup = words.mapToPair(tupleFunction);
		JavaPairRDD<String, String> joint = wordsTup.reduceByKey(concatFunction);
		JavaPairRDD<String,String> ordered = joint.flatMapValues(sortFunction);
		JavaPairRDD<String, String> ordered2 = ordered.sortByKey();

		// Save the word n to a text file (initiates evaluation)
		ordered2.saveAsTextFile(outputFilename);

		// Shut down the context
		sc.stop();
	}
}

	