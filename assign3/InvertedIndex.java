package com.refactorlabs.cs378.assign3;

// Imports
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;

public class InvertedIndex extends Configured implements Tool {

	public final static Text ONE = new Text();

	// Map and Reduce classes
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

		private static final String MAPPER_COUNTER_GROUP = "Mapper Counts";
		private Text verse_1 = new Text();
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			List<String> arr_words = new ArrayList<String>();
			String line = value.toString();
			if(line == "");
			else {
					line = line.replaceAll("[,.;_&*?!()]", "");
					line = line.replaceAll("[\"]", "");
					line = line.replaceAll("  ", " ");
					String arr[] = line.split(" ");
					verse_1.set(arr[0]);

					//Check for the duplicates
					for (int i = 1; i < arr.length; ++i) {
							arr[i] = arr[i].replaceAll(":","");
							arr[i] = arr[i].toLowerCase();
							if (arr_words.contains(arr[i])) ;
							else
								arr_words.add(arr[i]);
					}
				}
				//Insert the words as text into the mapper
				for (String words : arr_words) {
					word.set(words);
					context.write(word, verse_1);
					// For each word in the input line, emit a count of 1 for that word.
					context.getCounter(MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
				}
		}
	}

	public static class CombinerClass extends  Reducer<Text, Text, Text, Text> {
		private static final String COMBINER_COUNTER_GROUP = "Combiner Counts";

		private Text word = new Text();

		public void combine(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

			context.getCounter(COMBINER_COUNTER_GROUP, "verses out").increment(1L);
			String temp = "";
			for (Text value : values){
				 temp = temp + value.toString() + ",";
				}
				word.set(temp);
				context.write(key, word);
			}
		}

	public static class ReduceClass extends Reducer<Text,Text, Text,Text> {

		private static final String REDUCER_COUNTER_GROUP = "Reducer Counts";
		private Text word = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

			context.getCounter(REDUCER_COUNTER_GROUP, "verses out").increment(1L);
			//Concat the string
			String temp = "";
			for (Text value : values){
				temp = temp + value.toString() + ",";
			}

			//Splitting the verse to sort it
			String lines[] = temp.split(",");
			//Lexographical sort
			Arrays.sort(lines);
			//Complete Sort
			Arrays.sort(lines, new Comparator<String>() {
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

			//Concatenate the list of sorted verses
			temp = "";
			for(int i = 0; i < lines.length; ++i) {
				if (i == lines.length - 1)
					temp += lines[i];
				else
					temp = temp + lines[i] + ",";
			}
			word.set(temp);
			context.write(key, word);
		}
	}

		/**
		 * The run method specifies the characteristics of the map-reduce job
		 * by setting values on the Job object, and then initiates the map-reduce
		 * job and waits for it to complete.
		 */
		public int run(String[] args) throws Exception {
			Configuration conf = getConf();
			String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

			Job job = Job.getInstance(conf, "InvertedIndex");
			// Identify the JAR file to replicate to all machines.
			job.setJarByClass(InvertedIndex.class);

			// Set the output key and value types (for map and reduce).
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			// Set the map and reduce classes (and combiner, if used).
			job.setMapperClass(MapClass.class);
			job.setReducerClass(ReduceClass.class);
			job.setCombinerClass(CombinerClass.class);

			// Set the input and output file formats.
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			// Grab the input file and output directory from the command line.
			FileInputFormat.addInputPaths(job, appArgs[0]);
			FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

			// Initiate the map-reduce job, and wait for completion.
			job.waitForCompletion(true);

			return 0;
		}

		public static void main(String[] args) throws Exception {
			int res = ToolRunner.run(new InvertedIndex(), args);
			System.exit(res);
		}
	}