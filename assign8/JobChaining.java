package com.refactorlabs.cs378.assign8;

import com.refactorlabs.cs378.assign7.SessionType;
import com.refactorlabs.cs378.sessions.Event;
import com.refactorlabs.cs378.sessions.EventSubtype;
import com.refactorlabs.cs378.sessions.Session;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;

/**
 * Created by Kaivan on 10/13/2016.
 */
/* Use assignment 5 UserSessions to create .avro file for dataset7!
    User .avro as an input here and output a .avro file after filtering out stuff
* */
public class JobChaining extends Configured implements Tool {

    //1st Mapper
    public static class MapClass extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, AvroKey<CharSequence>, AvroValue<Session>> {

        private Text word = new Text();
        private AvroMultipleOutputs MultipleOutputs;

        public void setup(Context context) {
            MultipleOutputs = new AvroMultipleOutputs(context);
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            MultipleOutputs.close();
        }

        //Clean up function: we have to close the multiple output objects. SOmetimes when we dont close it, it doesnt get written out.
        // that would give me multiple output files.
        @Override
        public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
                throws IOException, InterruptedException {

            //MultipleOutputs = new AvroMultipleOutputs(context);
            List<Event> ev_list = value.datum().getEvents();
            List<SessionType> events = new ArrayList<SessionType>();

            if (ev_list.size() < 100) {
                for (Event e : ev_list) {
                    if (e.getEventType().toString().equalsIgnoreCase("CHANGE") || e.getEventType().toString().equalsIgnoreCase("EDIT")) {
                        if (e.getEventSubtype().toString().equalsIgnoreCase("CONTACT_FORM")) {
                            events.add(SessionType.SUBMITTER);
                        }
                    } else if (e.getEventType().toString().equalsIgnoreCase("CLICK")) {
                        events.add(SessionType.CLICKER);
                    } else if (e.getEventType().toString().equalsIgnoreCase("SHOW") || e.getEventType().toString().equalsIgnoreCase("DISPLAY")) {
                        events.add(SessionType.SHOWER);
                    } else if (e.getEventType().toString().equalsIgnoreCase("VISIT")) {
                        events.add(SessionType.VISITOR);
                    } else {
                        events.add(SessionType.OTHER);
                    }
                }
            } else {
                context.getCounter("Mapper Counts", "Large Sessions discarded").increment(1L);
                return;
            }

            if (events.contains(SessionType.SUBMITTER)) {
                MultipleOutputs.write(SessionType.SUBMITTER.getText(), key, value);
                context.getCounter(SessionType.SUBMITTER).increment(1L);
            } else if (events.contains(SessionType.CLICKER)) {
                MultipleOutputs.write(SessionType.CLICKER.getText(), key, value);
                context.getCounter(SessionType.CLICKER).increment(1L);
            } else if (events.contains(SessionType.SHOWER)) {
                MultipleOutputs.write(SessionType.SHOWER.getText(), key, value);
                context.getCounter(SessionType.SHOWER).increment(1L);
            } else if (events.contains(SessionType.VISITOR)) {
                MultipleOutputs.write(SessionType.VISITOR.getText(), key, value);
                context.getCounter(SessionType.VISITOR).increment(1L);
            } else {
                MultipleOutputs.write(SessionType.OTHER.getText(), key, value);
                context.getCounter(SessionType.OTHER).increment(1L);
            }
        }
    }

    public static class SubmitterMap extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, AvroKey<EventSubtypeStatisticsKey>, AvroValue<EventSubtypeStatisticsData>> {

        public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
                throws IOException, InterruptedException {
            List<Event> ev_list = value.datum().getEvents();
            HashMap<EventSubtype, Long> stats = new HashMap<EventSubtype, Long>();


            for (EventSubtype subType : EventSubtype.values()) {
                stats.put(subType, 0L);
            }

            for (Event e : ev_list) {
                EventSubtype subType = e.getEventSubtype();
                Long currentVal = stats.get(subType);
                stats.put(subType, currentVal + 1);
            }

            for (EventSubtype subType : stats.keySet()) {
                EventSubtypeStatisticsKey.Builder val_key = EventSubtypeStatisticsKey.newBuilder();
                val_key.setSessionType(SessionType.SUBMITTER.getText());
                val_key.setEventSubtype(subType.toString());

                EventSubtypeStatisticsData.Builder val_data = EventSubtypeStatisticsData.newBuilder();
                long n = stats.get(subType);

                val_data.setSessionCount(1L);
                val_data.setTotalCount(n);
                val_data.setSumOfSquares(n * n);
                val_data.setMean(0.0);
                val_data.setVariance(0.0);

                context.write(new AvroKey<EventSubtypeStatisticsKey>(val_key.build()), new AvroValue<EventSubtypeStatisticsData>(val_data.build()));
            }
        }
    }

    public static class ClickerMap extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, AvroKey<EventSubtypeStatisticsKey>, AvroValue<EventSubtypeStatisticsData>> {

        public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
                throws IOException, InterruptedException {
            HashMap<EventSubtype, Long> stats = new HashMap<EventSubtype, Long>();
            List<Event> ev_list = value.datum().getEvents();

            for (EventSubtype subType : EventSubtype.values())
                stats.put(subType, 0L);

            for (Event e : ev_list) {
                EventSubtype subType = e.getEventSubtype();
                Long currentVal = stats.get(subType);
                stats.put(subType, currentVal + 1);
            }

            for (EventSubtype subType : stats.keySet()) {
                EventSubtypeStatisticsKey.Builder val_key = EventSubtypeStatisticsKey.newBuilder();
                val_key.setSessionType(SessionType.CLICKER.getText());
                val_key.setEventSubtype(subType.toString());

                EventSubtypeStatisticsData.Builder val_data = EventSubtypeStatisticsData.newBuilder();
                long n = stats.get(subType);

                val_data.setSessionCount(1L);
                val_data.setTotalCount(n);
                val_data.setSumOfSquares(n * n);
                val_data.setMean(0.0);
                val_data.setVariance(0.0);

                context.write(new AvroKey<EventSubtypeStatisticsKey>(val_key.build()), new AvroValue<EventSubtypeStatisticsData>(val_data.build()));
            }
        }
    }

    public static class ShowerMap extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, AvroKey<EventSubtypeStatisticsKey>, AvroValue<EventSubtypeStatisticsData>> {

        public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
                throws IOException, InterruptedException {
            List<Event> ev_list = value.datum().getEvents();
            HashMap<EventSubtype, Long> stats = new HashMap<EventSubtype, Long>();


            for (EventSubtype subType : EventSubtype.values()) {
                stats.put(subType, 0L);
            }

            for (Event e : ev_list) {
                EventSubtype subType = e.getEventSubtype();
                Long currentVal = stats.get(subType);
                stats.put(subType, currentVal + 1);
            }

            for (EventSubtype subType : stats.keySet()) {
                EventSubtypeStatisticsKey.Builder val_key = EventSubtypeStatisticsKey.newBuilder();
                val_key.setSessionType(SessionType.SHOWER.getText());
                val_key.setEventSubtype(subType.toString());

                EventSubtypeStatisticsData.Builder val_data = EventSubtypeStatisticsData.newBuilder();
                long n = stats.get(subType);

                val_data.setSessionCount(1L);
                val_data.setTotalCount(n);
                val_data.setSumOfSquares(n * n);
                val_data.setMean(0.0);
                val_data.setVariance(0.0);

                context.write(new AvroKey<EventSubtypeStatisticsKey>(val_key.build()), new AvroValue<EventSubtypeStatisticsData>(val_data.build()));
            }
        }
    }

    public static class VisitorMap extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, AvroKey<EventSubtypeStatisticsKey>, AvroValue<EventSubtypeStatisticsData>> {

        public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
                throws IOException, InterruptedException {
            List<Event> ev_list = value.datum().getEvents();
            HashMap<EventSubtype, Long> stats = new HashMap<EventSubtype, Long>();


            for (EventSubtype subType : EventSubtype.values()) {
                stats.put(subType, 0L);
            }

            for (Event e : ev_list) {
                EventSubtype subType = e.getEventSubtype();
                Long currentVal = stats.get(subType);
                stats.put(subType, currentVal + 1);
            }

            for (EventSubtype subType : stats.keySet()) {
                EventSubtypeStatisticsKey.Builder val_key = EventSubtypeStatisticsKey.newBuilder();
                val_key.setSessionType(SessionType.VISITOR.getText());
                val_key.setEventSubtype(subType.toString());

                EventSubtypeStatisticsData.Builder val_data = EventSubtypeStatisticsData.newBuilder();
                long n = stats.get(subType);

                val_data.setSessionCount(1L);
                val_data.setTotalCount(n);
                val_data.setSumOfSquares(n * n);
                val_data.setMean(0.0);
                val_data.setVariance(0.0);

                context.write(new AvroKey<EventSubtypeStatisticsKey>(val_key.build()), new AvroValue<EventSubtypeStatisticsData>(val_data.build()));
            }
        }
    }


    public static class ReduceClass extends Reducer<AvroKey<EventSubtypeStatisticsKey>, AvroValue<EventSubtypeStatisticsData>, AvroKey<EventSubtypeStatisticsKey>, AvroValue<EventSubtypeStatisticsData>> {

        @Override
        public void reduce(AvroKey<EventSubtypeStatisticsKey> key, Iterable<AvroValue<EventSubtypeStatisticsData>> values, Context context)
                throws IOException, InterruptedException {
            long total_count = 0L;
            long session_count = 0L;
            long sum_of_squares = 0L;

            for (AvroValue<EventSubtypeStatisticsData> value : values) {
                total_count = total_count + value.datum().getTotalCount();
                session_count = session_count + value.datum().getSessionCount();
                sum_of_squares = sum_of_squares + value.datum().getSumOfSquares();
            }

            double mean = (double) total_count / session_count;
            double variance = (double) sum_of_squares / session_count - mean * mean;

            EventSubtypeStatisticsData.Builder stat = EventSubtypeStatisticsData.newBuilder();
            stat.setTotalCount(total_count);
            stat.setSessionCount(session_count);
            stat.setSumOfSquares(sum_of_squares);
            stat.setMean(mean);
            stat.setVariance(variance);
            context.write(key, new AvroValue<EventSubtypeStatisticsData>(stat.build()));
        }
    }


    public static class Aggregator extends Mapper<AvroKey<EventSubtypeStatisticsKey>, AvroValue<EventSubtypeStatisticsData>,
            AvroKey<EventSubtypeStatisticsKey>, AvroValue<EventSubtypeStatisticsData>> {
        @Override
        public void map(AvroKey<EventSubtypeStatisticsKey> key, AvroValue<EventSubtypeStatisticsData> value, Context context)
                throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
        Job jobBin = Job.getInstance(conf, "JobChaining");
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        jobBin.setJarByClass(JobChaining.class);
        jobBin.setInputFormatClass(AvroKeyValueInputFormat.class);
        AvroJob.setInputKeySchema(jobBin, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(jobBin, Session.getClassSchema());
        jobBin.setMapperClass(MapClass.class);
        AvroJob.setOutputKeySchema(jobBin, Schema.create(Schema.Type.STRING));
        AvroJob.setOutputValueSchema(jobBin, Session.getClassSchema());
        jobBin.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        for (SessionType type : SessionType.values()) {
            AvroMultipleOutputs.addNamedOutput(jobBin, type.getText(), AvroKeyValueOutputFormat.class, Schema.create(Schema.Type.STRING), Session.getClassSchema());
        }
        AvroMultipleOutputs.setCountersEnabled(jobBin, true);
        jobBin.setNumReduceTasks(0);

        FileInputFormat.addInputPath(jobBin, new Path(appArgs[0]));
        FileOutputFormat.setOutputPath(jobBin, new Path(appArgs[1]));
        jobBin.waitForCompletion(true);

        Configuration confOne = getConf();
        confOne.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
        Job jobOne = Job.getInstance(confOne, "JobChaining");
        jobOne.setJarByClass(JobChaining.class);
        jobOne.setInputFormatClass(AvroKeyValueInputFormat.class);
        AvroJob.setInputKeySchema(jobOne, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(jobOne, Session.getClassSchema());

        jobOne.setMapperClass(SubmitterMap.class);
        AvroJob.setMapOutputKeySchema(jobOne, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setMapOutputValueSchema(jobOne, EventSubtypeStatisticsData.getClassSchema());
        jobOne.setOutputFormatClass(AvroKeyValueOutputFormat.class);

        jobOne.setReducerClass(ReduceClass.class);
        AvroJob.setOutputKeySchema(jobOne, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setOutputValueSchema(jobOne, EventSubtypeStatisticsData.getClassSchema());

        FileInputFormat.addInputPath(jobOne, new Path(appArgs[1] + "/submitter-m-00000.avro"));
        FileOutputFormat.setOutputPath(jobOne, new Path(appArgs[1] + "/submitter"));

        jobOne.submit();

        Configuration confTwo = getConf();
        confTwo.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
        Job jobTwo = Job.getInstance(confTwo, "JobChaining");
        jobTwo.setJarByClass(JobChaining.class);
        jobTwo.setInputFormatClass(AvroKeyValueInputFormat.class);
        AvroJob.setInputKeySchema(jobTwo, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(jobTwo, Session.getClassSchema());

        jobTwo.setMapperClass(VisitorMap.class);
        AvroJob.setMapOutputKeySchema(jobTwo, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setMapOutputValueSchema(jobTwo, EventSubtypeStatisticsData.getClassSchema());
        jobTwo.setOutputFormatClass(AvroKeyValueOutputFormat.class);

        jobTwo.setReducerClass(ReduceClass.class);
        AvroJob.setOutputKeySchema(jobTwo, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setOutputValueSchema(jobTwo, EventSubtypeStatisticsData.getClassSchema());

        FileInputFormat.addInputPath(jobTwo, new Path(appArgs[1] + "/visitor-m-00000.avro"));
        FileOutputFormat.setOutputPath(jobTwo, new Path(appArgs[1] + "/visitor"));

        jobTwo.submit();

        Configuration confThree = getConf();
        confThree.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
        Job jobThree = Job.getInstance(confThree, "JobChaining");
        jobThree.setJarByClass(JobChaining.class);
        jobThree.setInputFormatClass(AvroKeyValueInputFormat.class);
        AvroJob.setInputKeySchema(jobThree, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(jobThree, Session.getClassSchema());

        jobThree.setMapperClass(ShowerMap.class);
        AvroJob.setMapOutputKeySchema(jobThree, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setMapOutputValueSchema(jobThree, EventSubtypeStatisticsData.getClassSchema());
        jobThree.setOutputFormatClass(AvroKeyValueOutputFormat.class);

        jobThree.setReducerClass(ReduceClass.class);
        AvroJob.setOutputKeySchema(jobThree, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setOutputValueSchema(jobThree, EventSubtypeStatisticsData.getClassSchema());

        FileInputFormat.addInputPath(jobThree, new Path(appArgs[1] + "/shower-m-00000.avro"));
        FileOutputFormat.setOutputPath(jobThree, new Path(appArgs[1] + "/shower"));

        jobThree.submit();

        Configuration confFour = getConf();
        confFour.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
        Job jobFour = Job.getInstance(confFour, "JobChaining");
        jobFour.setJarByClass(JobChaining.class);
        jobFour.setInputFormatClass(AvroKeyValueInputFormat.class);
        AvroJob.setInputKeySchema(jobFour, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(jobFour, Session.getClassSchema());

        jobFour.setMapperClass(ClickerMap.class);
        AvroJob.setMapOutputKeySchema(jobFour, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setMapOutputValueSchema(jobFour, EventSubtypeStatisticsData.getClassSchema());
        jobFour.setOutputFormatClass(AvroKeyValueOutputFormat.class);

        jobFour.setReducerClass(ReduceClass.class);
        AvroJob.setOutputKeySchema(jobFour, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setOutputValueSchema(jobFour, EventSubtypeStatisticsData.getClassSchema());

        FileInputFormat.addInputPath(jobFour, new Path(appArgs[1] + "/clicker-m-00000.avro"));
        FileOutputFormat.setOutputPath(jobFour, new Path(appArgs[1] + "/clicker"));

        jobFour.submit();

        while (!jobOne.isComplete() || !jobTwo.isComplete() || !jobThree.isComplete() || !jobFour.isComplete()) {
            Thread.sleep(200L);
        }
        Job aggregateJob = Job.getInstance(conf, "aggregateJob");
        aggregateJob.setJarByClass(JobChaining.class);
        aggregateJob.setMapperClass(Aggregator.class);
        aggregateJob.setInputFormatClass(AvroKeyValueInputFormat.class);
        aggregateJob.setOutputFormatClass(TextOutputFormat.class);
        AvroJob.setMapOutputKeySchema(aggregateJob, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setMapOutputValueSchema(aggregateJob, EventSubtypeStatisticsData.getClassSchema());
        aggregateJob.setReducerClass(ReduceClass.class);
        AvroJob.setInputKeySchema(aggregateJob, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setInputValueSchema(aggregateJob, EventSubtypeStatisticsData.getClassSchema());
        AvroJob.setOutputKeySchema(aggregateJob, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setOutputValueSchema(aggregateJob, EventSubtypeStatisticsData.getClassSchema());

        FileInputFormat.addInputPaths(aggregateJob, appArgs[1] + "/submitter/*.avro");
        FileInputFormat.addInputPaths(aggregateJob, appArgs[1] + "/visitor/*.avro");
        FileInputFormat.addInputPaths(aggregateJob, appArgs[1] + "/shower/*.avro");
        FileInputFormat.addInputPaths(aggregateJob, appArgs[1] + "/clicker/*.avro");
        FileOutputFormat.setOutputPath(aggregateJob, new Path(appArgs[1] + "/aggregator"));

        aggregateJob.waitForCompletion(true);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new JobChaining(), args);
        System.exit(res);
    }
}

