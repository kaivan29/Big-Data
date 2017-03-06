package com.refactorlabs.cs378.assign7;

import com.refactorlabs.cs378.sessions.Event;
import com.refactorlabs.cs378.sessions.Session;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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
public class FilterSession extends Configured implements Tool {
    public static class MapClass extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, AvroKey<CharSequence>, AvroValue<Session>> {

        private Text word = new Text();
        private AvroMultipleOutputs MultipleOutputs;

        public void setup(Context context) {
            MultipleOutputs = new AvroMultipleOutputs(context);
        }

        public void cleanup(Context context) throws IOException, InterruptedException{
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

            if(ev_list.size() < 100) {
                for (Event e : ev_list) {
                    if (e.getEventType().toString().equalsIgnoreCase("CHANGE") || e.getEventType().toString().equalsIgnoreCase("EDIT")) {
                        if (e.getEventSubtype().toString().equalsIgnoreCase("CONTACT_FORM")) {
                            events.add(SessionType.SUBMITTER);
                        }
                    }
                    else if (e.getEventType().toString().equalsIgnoreCase("CLICK")) {
                        events.add(SessionType.CLICKER);
                    }
                    else if (e.getEventType().toString().equalsIgnoreCase("SHOW") || e.getEventType().toString().equalsIgnoreCase("DISPLAY")) {
                        events.add(SessionType.SHOWER);
                    }
                    else if (e.getEventType().toString().equalsIgnoreCase("VISIT")){
                        events.add(SessionType.VISITOR);
                    }
                    else {
                        events.add(SessionType.OTHER);
                    }
                }
            }
            else {
                context.getCounter("Mapper Counts", "Large Sessions discarded").increment(1L);
                return;
            }

            if(events.contains(SessionType.SUBMITTER)){
                MultipleOutputs.write(SessionType.SUBMITTER.getText(), key, value);
                context.getCounter(SessionType.SUBMITTER).increment(1L);
            }
            else if(events.contains(SessionType.CLICKER)) {
                Random rand = new Random();
                int  n = rand.nextInt(100) + 1;
                if(n < 11){
                    MultipleOutputs.write(SessionType.CLICKER.getText(), key, value);
                    context.getCounter(SessionType.CLICKER).increment(1L);
                }
                else
                    context.getCounter("Mapper Counts", "Clicker Session Discarded").increment(1L);
            }
            else if(events.contains(SessionType.SHOWER)){
                Random rand = new Random();
                int  n = rand.nextInt(100) + 1;
                if(n < 3){
                    MultipleOutputs.write(SessionType.SHOWER.getText(), key, value);
                    context.getCounter(SessionType.SHOWER).increment(1L);
                }
                else
                    context.getCounter("Mapper Counts", "Shower Session Discarded").increment(1L);
            }
            else if(events.contains(SessionType.VISITOR)){
                MultipleOutputs.write(SessionType.VISITOR.getText(), key, value);
                context.getCounter(SessionType.VISITOR).increment(1L);
            }
            else{
                MultipleOutputs.write(SessionType.OTHER.getText(), key, value);
            }
        }
    }

    /**
     * The run() method is called (indirectly) from main(), and contains all the job
     * setup and configuration.
     */
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        // Use this JAR first in the classpath (We also set a bootstrap script in AWS)
        conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

        Job job = Job.getInstance(conf, "UserSessions");
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(FilterSession.class);

        job.setInputFormatClass(AvroKeyValueInputFormat.class);
        AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(job, Session.getClassSchema());
        job.setMapperClass(MapClass.class);

        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setOutputValueSchema(job, Session.getClassSchema());
        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);

        for(SessionType sessionType : SessionType.values()){
            AvroMultipleOutputs.addNamedOutput(job, sessionType.getText(), AvroKeyValueOutputFormat.class,
                                                Schema.create(Schema.Type.STRING), Session.getClassSchema());
        }
        AvroMultipleOutputs.setCountersEnabled(job, true);

        job.setNumReduceTasks(0);
        // Grab the input file and output directory from the command line.
        FileInputFormat.addInputPaths(job, appArgs[0]);
        FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

        // Initiate the map-reduce job, and wait for completion.
        job.waitForCompletion(true);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new FilterSession(), args);
        System.exit(res);
    }
}
