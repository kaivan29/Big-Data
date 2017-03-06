package com.refactorlabs.cs378.assign4;

import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
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
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Created by Kaivan on 9/23/2016.
 */
public class WordStatistics extends Configured implements Tool {

    public static class MapClass extends Mapper<LongWritable, Text, Text, AvroValue<WordStatisticsData>> {

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

            long paragraphLength = tokenizer.countTokens();

            Map<String, Long> wordstat = new HashMap<String, Long>();

            while(tokenizer.hasMoreTokens()) {
                String temp = tokenizer.nextToken();
                if(wordstat.containsKey(temp)) {
                    wordstat.put(temp, wordstat.get(temp) + 1);
                }
                else {
                    wordstat.put(temp, 1L);
                }
            }

            for(String keys: wordstat.keySet()) {
                long dos = wordstat.get(keys)*wordstat.get(keys);
                word.set(keys);
                WordStatisticsData.Builder builder = WordStatisticsData.newBuilder();
                builder.setWordCount(wordstat.get(keys));
                builder.setParagraphCount(1D);
                builder.setDoubleOfSquares((double)dos);
                builder.setMin(Long.MAX_VALUE);
                builder.setMax(Long.MIN_VALUE);
                builder.setMean(0D);
                builder.setVariance(0D);
                context.write(word, new AvroValue<WordStatisticsData>(builder.build()));
            }

            word = new Text("Paragraph length");
            WordStatisticsData.Builder builder = WordStatisticsData.newBuilder();
            double dos = paragraphLength*paragraphLength;
            builder.setParagraphCount(1D);
            builder.setWordCount(paragraphLength);
            builder.setDoubleOfSquares(dos);
            builder.setMin(Long.MAX_VALUE);
            builder.setMax(Long.MIN_VALUE);
            builder.setMean(0D);
            builder.setVariance(0D);
            context.write(word, new AvroValue<WordStatisticsData>(builder.build()));
        }
    }

    public static class ReduceClass extends Reducer<Text, AvroValue<WordStatisticsData>, Text, AvroValue<WordStatisticsData>> {

        @Override
        public void reduce(Text key, Iterable<AvroValue<WordStatisticsData>> values, Context context)
                throws IOException, InterruptedException {

            long wc = 0L;
            double pc = 0D;
            double dos = 0D;
            long min = Long.MAX_VALUE;
            long max =Long.MIN_VALUE;


            for (AvroValue<WordStatisticsData> value : values) {
                wc += value.datum().getWordCount();
                pc += value.datum().getParagraphCount();
                dos += value.datum().getDoubleOfSquares();

                if( max < value.datum().getWordCount())
                    max = value.datum().getWordCount();
                if(value.datum().getWordCount() < min)
                    min = value.datum().getWordCount();
            }

            double mean = (double) wc/pc;
            double variance = dos/pc - mean*mean;

            // Emit the total count for the word.
            WordStatisticsData.Builder builder = WordStatisticsData.newBuilder();
            builder.setParagraphCount(pc);
            builder.setWordCount(wc);
            builder.setDoubleOfSquares(dos);
            builder.setMin(min);
            builder.setMax(max);
            builder.setMean(mean);
            builder.setVariance(variance);
            context.write(key, new AvroValue<WordStatisticsData>(builder.build()));
        }
    }

    /**
     * The run() method is called (indirectly) from main(), and contains all the job
     * setup and configuration.
     */
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WordStatistics <input path> <output path>");
            return -1;
        }
        Configuration conf = getConf();
        // Use this JAR first in the classpath (We also set a bootstrap script in AWS)
        conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

        Job job = Job.getInstance(conf, "WordStatistics");
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(WordStatistics.class);

        // Specify the Map
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(MapClass.class);
        job.setMapOutputKeyClass(Text.class);
        AvroJob.setMapOutputValueSchema(job, WordStatisticsData.getClassSchema());

        // Specify the Reduce
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setReducerClass(ReduceClass.class);
        job.setOutputKeyClass(Text.class);
        AvroJob.setOutputValueSchema(job, WordStatisticsData.getClassSchema());

        // Grab the input file and output directory from the command line.
        FileInputFormat.addInputPaths(job, appArgs[0]);
        FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

        // Initiate the map-reduce job, and wait for completion.
        job.waitForCompletion(true);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new WordStatistics(), args);
        System.exit(res);
    }
}
