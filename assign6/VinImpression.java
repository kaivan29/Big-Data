package com.refactorlabs.cs378.assign6;

//import com.refactorlabs.cs378.sessions.Event;
//import com.refactorlabs.cs378.sessions.Session;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Kaivan on 10/10/2016.
 */
/*
type, subtype, vin
mapper output - text is the vin
unique_users = 1;
clicks - if the user clicked or not
if the user clicks, we need the subtype
edit_contact form - if the type is edit and subtype is contact form
vic = (1,0/(1 and subtype))
        */
public class VinImpression extends Configured implements Tool {

    public static class Map1Class extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, Text, AvroValue<VinImpressionCounts>> {

        private Text word = new Text();

        @Override
        public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
                throws IOException, InterruptedException {

            List<Event> ev_list = value.datum().getEvents();
            HashMap<String, Long> uniqueUsers = new HashMap<String, Long>();
            HashMap<String, Long> contactForms = new HashMap<String, Long>();
            HashMap<String, Map<CharSequence, Long>> Clicks = new HashMap<String, Map<CharSequence, Long>>();


            for(Event e : ev_list)
            {
                String vin = e.getVin().toString();
                uniqueUsers.put(vin, 1L);
                if (e.getEventType() == EventType.CLICK) {

                    if (Clicks.containsKey(vin)) {
                        Map<CharSequence, Long> clickMap = Clicks.get(vin);
                        clickMap.put(e.getEventSubtype().name(), 1L);
                        Clicks.put(vin, clickMap);
                    }
                    else{
                        Map<CharSequence, Long> clickMap = new HashMap<CharSequence, Long>();
                        clickMap.put(e.getEventSubtype().name(), 1L);
                        Clicks.put(vin, clickMap);
                    }
                }
                if (e.getEventType() == EventType.EDIT && e.getEventSubtype() == EventSubtype.CONTACT_FORM) {
                    contactForms.put(vin, 1L);
                }
            }

            for (String vin : uniqueUsers.keySet()) {
                VinImpressionCounts.Builder vinBuilder = VinImpressionCounts.newBuilder();
                vinBuilder.setUniqueUsers(1L);
                if (contactForms.containsKey(vin))
                    vinBuilder.setEditContactForm(1L);
                if (Clicks.containsKey(vin)) {
                    vinBuilder.setClicks(Clicks.get(vin));
                }
                context.write(new Text(vin), new AvroValue<VinImpressionCounts>(vinBuilder.build()));
            }
        }
    }

    public static class Map2Class extends Mapper<LongWritable, Text, Text, AvroValue<VinImpressionCounts>> {

        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String lines[] = line.split(",");

            VinImpressionCounts.Builder vin = VinImpressionCounts.newBuilder();
            if(lines[1].equalsIgnoreCase("SRP"))
                vin.setMarketplaceSrps(Long.valueOf(lines[2]));
            else
                vin.setMarketplaceVdps(Long.valueOf(lines[2]));
            //key
            word.set(lines[0]);
            context.write(word, new AvroValue<VinImpressionCounts>(vin.build()));
        }
    }

    public static class ReduceClass extends Reducer<Text, AvroValue<VinImpressionCounts>, AvroKey<CharSequence>, AvroValue<VinImpressionCounts>> {

        //@Override
        public void reduce(Text key, Iterable<AvroValue<VinImpressionCounts>> values, Context context)
                throws IOException, InterruptedException {

            VinImpressionCounts.Builder val = VinImpressionCounts.newBuilder();
            long u_users = 0L;
            long vdps = 0L;
            long srps = 0L;
            long c_form = 0L;
            Map<CharSequence, Long> click = new HashMap<CharSequence, Long>();

            for(AvroValue<VinImpressionCounts> value : values) {

                u_users +=value.datum().getUniqueUsers();
                srps += value.datum().getMarketplaceSrps();
                vdps += value.datum().getMarketplaceVdps();
                c_form += value.datum().getEditContactForm();

                if(value.datum().getClicks() != null) {
                    for (CharSequence c : value.datum().getClicks().keySet()) {
                        if (click.containsKey(c))
                            click.put(c, value.datum().getClicks().get(c) + click.get(c));
                        else
                            click.put(c, value.datum().getClicks().get(c));
                    }
                }
            }
            val.setUniqueUsers(u_users);
            val.setEditContactForm(c_form);
            val.setMarketplaceVdps(vdps);
            val.setMarketplaceSrps(srps);
            val.setClicks(click);

            CharSequence word = key.toString();
            if(val.getUniqueUsers() > 0)
                context.write(new AvroKey<CharSequence>(word), new AvroValue<VinImpressionCounts>(val.build()));
        }
    }
    public int run(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println("Usage");
            return -1;
        }

        Configuration conf = getConf();
        // Use this JAR first in the classpath (We also set a bootstrap script in AWS)
        conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

        Job job = Job.getInstance(conf, "VinImpression");
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(VinImpression.class);

        // Specify the Map
        Path mapper1 = new Path(appArgs[0]);
        Path mapper2 = new Path(appArgs[1]);
        MultipleInputs.addInputPath(job, mapper1, AvroKeyValueInputFormat.class, Map1Class.class);
        MultipleInputs.addInputPath(job, mapper2, TextInputFormat.class, Map2Class.class);

        AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(job, Session.getClassSchema());
        job.setMapOutputKeyClass(Text.class);
        AvroJob.setMapOutputValueSchema(job, VinImpressionCounts.getClassSchema());

        // Specify the Reduce
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setReducerClass(ReduceClass.class);
        job.setOutputKeyClass(CharSequence.class);
        AvroJob.setOutputValueSchema(job, VinImpressionCounts.getClassSchema());

        // Grab the input file and output directory from the command line.
        //FileInputFormat.addInputPaths(job, appArgs[0]);
        FileOutputFormat.setOutputPath(job, new Path(appArgs[2]));

        // Initiate the map-reduce job, and wait for completion.
        job.waitForCompletion(true);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new VinImpression(), args);
        System.exit(res);
    }
}
