package com.refactorlabs.cs378.assign2;

import com.refactorlabs.cs378.assign2.WordStatisticsWritable;
import com.sun.corba.se.spi.ior.Writeable;
import org.apache.hadoop.conf.Configuration;
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

import java.io.IOException;
import java.util.*;

/**
 * Example MapReduce program that performs word count.
 *
 * @author David Franke (dfranke@cs.utexas.edu)
 */
public class WordStatistics {

	/**
	 * Each count output from the map() function is "1", so to minimize small
	 * object creation we can use a constant for this output value/object.
	 */
	//public final static LongWritable ONE = new LongWritable(1L);

	/**
	 * The Map class for word count.  Extends class Mapper, provided by Hadoop.
	 * This class defines the map() function for the word count example.
	 */
	public static class MapClass extends Mapper<LongWritable, Text, Text, WordStatisticsWritable> {

		/**
		 * Counter group for the mapper.  Individual counters are grouped for the mapper.
		 */
		private static final String MAPPER_COUNTER_GROUP = "Mapper Counts";

		/**
		 * Local variable "word" will contain the word identified in the input.
		 * The Hadoop Text object is mutable, so we can reuse the same object and
		 * simply reset its value as each word in the input is encountered.
		 */
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			line = line.replaceAll("[,.;_&*\":?!]", " ");
			line = line.replaceAll("[\\[]"," [");
			line = line.replaceAll("--"," ");
			line = line.toLowerCase();
			StringTokenizer tokenizer = new StringTokenizer(line);

			context.getCounter(MAPPER_COUNTER_GROUP, "Input Lines").increment(1L);

			Map<String, Long> wordstat = new HashMap<String, Long>();

			// For each word in the input line, emit a count of 1 for that word.
			while (tokenizer.hasMoreTokens()) {
				String temp = tokenizer.nextToken();
				if(wordstat.containsKey(temp)) {
					wordstat.put(temp, wordstat.get(temp) + 1);
				}
				else {
					wordstat.put(temp, 1L);
				}
				context.getCounter(MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
			}

			for(String keys: wordstat.keySet()) {
				long dos = wordstat.get(keys)*wordstat.get(keys);
				word.set(keys);
				WordStatisticsWritable wsw = new WordStatisticsWritable();
				wsw.set_wordCount(wordstat.get(keys));
				wsw.set_paragraphCount(1D);
				wsw.set_doubleOfSquares((double)dos);
				context.write(word, wsw);
			}
		}
	}

	public static class CombinerClass extends Reducer<Text, WordStatisticsWritable, Text, WordStatisticsWritable> {

		/**
		 * Counter group for the reducer.  Individual counters are grouped for the reducer.
		 */
		private static final String COMBINER_COUNTER_GROUP = "Combiner Counts";

		public void combine(Text key, Iterable<WordStatisticsWritable> values, Context context)
				throws IOException, InterruptedException {
			long wc = 0L;
			double pc = 0D;
			double dos = 0D;

			context.getCounter(COMBINER_COUNTER_GROUP, "Words Out").increment(1L);

			// Sum up the counts for the current word, specified in object "key".
			for (WordStatisticsWritable value : values) {
				wc += value.get_wordCount();
				pc += value.get_paragraphCount();
				dos += value.get_doubleOfSquares();
			}
			// Emit the total count for the word.
			WordStatisticsWritable wsw = new WordStatisticsWritable();
			wsw.set_wordCount(wc);
			wsw.set_paragraphCount(pc);
			wsw.set_doubleOfSquares(dos);
			context.write(key, wsw);
		}
	}

	/**
	 * The Reduce class for word count.  Extends class Reducer, provided by Hadoop.
	 * This class defines the reduce() function for the word count example.
	 */
	public static class ReduceClass extends Reducer<Text, WordStatisticsWritable, Text, WordStatisticsWritable> {

		/**
		 * Counter group for the reducer.  Individual counters are grouped for the reducer.
		 */
		private static final String REDUCER_COUNTER_GROUP = "Reducer Counts";

		public void reduce(Text key, Iterable<WordStatisticsWritable> values, Context context)
				throws IOException, InterruptedException {

			long wc = 0L;
			double pc = 0D;
			double dos = 0D;

			context.getCounter(REDUCER_COUNTER_GROUP, "Words Out").increment(1L);

			// Sum up the counts for the current word, specified in object "key".
			for (WordStatisticsWritable value : values) {
				wc += value.get_wordCount();
				pc += value.get_paragraphCount();
				dos += value.get_doubleOfSquares();
			}

			double mean = (double) wc/pc;
			double variance = dos/pc - mean*mean;

			// Emit the total count for the word.
			WordStatisticsWritable wsw = new WordStatisticsWritable();
			wsw.set_wordCount((long) pc);
			wsw.set_paragraphCount(mean);
			wsw.set_doubleOfSquares(variance);
			context.write(key, wsw);
		}
	}

	/**
	 * The main method specifies the characteristics of the map-reduce job
	 * by setting values on the Job object, and then initiates the map-reduce
	 * job and waits for it to complete.
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		Job job = Job.getInstance(conf, "WordStatistics");
		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(WordStatistics.class);

		// Set the output key and value types (for map and reduce).
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(WordStatisticsWritable.class);

		// Set the map and reduce classes.
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setCombinerClass(CombinerClass.class);

		// Set the input and output file formats.
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Grab the input file and output directory from the command line.
		FileInputFormat.addInputPath(job, new Path(appArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

		// Initiate the map-reduce job, and wait for completion.
		job.waitForCompletion(true);
	}
}