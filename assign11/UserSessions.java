package com.refactorlabs.cs378.assign11;

import java.awt.Event;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;


import org.apache.spark.SparkConf;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.util.LongAccumulator;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import scala.Tuple2;
import scala.annotation.meta.field;
import scala.util.Random;

/**
 * UserSessions application for Spark.
 */


public class UserSessions {
	public static void main(String[] args) {
		
		String inputFilename = args[0];
		String outputFilename = args[1];
		
		SparkConf conf = new SparkConf().clone().setAppName(UserSessions.class.getName()).setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		SparkContext sc = jsc.sc();
		
		final LongAccumulator session_count = sc.longAccumulator("Sessions");
		final LongAccumulator shower_session_count = sc.longAccumulator("ShowerSessions");
		final LongAccumulator filtered_session_count = sc.longAccumulator("FilteredSssions");
		final LongAccumulator event_count = sc.longAccumulator("Events");
		final LongAccumulator event2_count = sc.longAccumulator("Events2");
		
		JavaRDD<String> input = jsc.textFile(inputFilename);
		
		PairFunction<String, SessionKey, Event> key_events = new PairFunction<String, SessionKey , Event>() {
				public Tuple2<SessionKey, Event> call(String line){
					String[] lines = line.split("\t");
					String id = lines[0];
					String domain = lines[3];
					SessionKey session_key = new SessionKey(id,domain);
					Event event = new Event(lines[1],lines[4]);
					
					return new Tuple2<SessionKey,Event>(session_key,event);
				}
		};
		
		Function<Iterable<Event>, Iterable<Event>> select_unique = new Function<Iterable<Event>, Iterable<Event>>() {
			public Iterable<Event> call(Iterable<Event>list_n){
				Set<Event> unique_events = Sets.newHashSet();
				for (Event e : list_n){
					unique_events.add(e);
				}
				event_count.add(unique_events.size());
				return Lists.newArrayList(unique_events);
			}
		};
		
		Function<Iterable<Event>, Iterable<Event>> order_events = new Function<Iterable<Event>,Iterable<Event>>(){
			
			public Iterable<Event> call(Iterable<Event> list_n){
				List<Event>event_list = Lists.newArrayList(list_n);
				Collections.sort(event_list, EVENT_COMPARATOR);	
				return event_list;
			}
		};
		
		final Function<Tuple2<SessionKey,Iterable<Event>>,Boolean> sample_shower_sessions = 
				new Function<Tuple2<SessionKey, Iterable<Event>>,Boolean>() {
					
				private Random random = new Random();
				
				public Boolean call(Tuple2<SessionKey,Iterable<Event>> session){
					session_count.add(1);
					int session_event_count = 0;
					for (Event e : session._2()){
						session_event_count++;
					}
					
					boolean show_or_display = false;
					for(Event e : session._2()){
						if(e.eventSubType.equals("contact form") || e.eventType.equals("clicker")){
							event2_count.add(session_event_count);
							return true;
					}else if (e.eventType.equals("show") || e.eventType.equals("display")){
						show_or_display = true;
					}
					
				}
				
				if(show_or_display){
					shower_session_count.add(1);
					if(random.nextDouble() > 0.1){
						filtered_session_count.add(1);
						return false;
					}
				}
				
				event2_count.add(session_event_count);
				return true;
		}
};

	JavaPairRDD<SessionKey, Event> keyed_events = input.mapToPair(key_events);
	JavaPairRDD<SessionKey, Iterable<Event>> grouped_events = keyed_events.groupByKey();
	JavaPairRDD<SessionKey, Iterable<Event>> unique_events = grouped_events.mapValues(select_unique);
	JavaPairRDD<SessionKey, Iterable<Event>> filtered_sessions = unique_events.filter(sample_shower_sessions);
	JavaPairRDD<SessionKey, Iterable<Event>> sorted_events = filtered_sessions.mapValues(order_events);
	JavaPairRDD<SessionKey, Iterable<Event>> sorted_sessions = sorted_events.sortByKey(SESSION_KEY_COMPARATOR);
	JavaPairRDD<SessionKey, Iterable<Event>> partitioned_sessions = sorted_sessions.partitionBy(SESSION_PARTITIONER);
	
	partitioned_sessions.saveAsTextFile(outputFilename);
	
	
	System.out.println("Session Count : " + session_count.value());
	System.out.println("SHOWER Session Count : " + shower_session_count.value());
	System.out.println("Filtered Shower Sessions : "+ filtered_session_count.value());
	System.out.println("Event Count : "+ event_count.value());
	System.out.println("Event2 Count : "+ event2_count.value());

}
	
	
	
  private static final SessionPartitioner SESSION_PARTITIONER = new SessionPartitioner();
	
	private static class SessionKey extends Tuple2<String, String> implements Comparable<SessionKey>{
		
		public SessionKey(String userId, String referringDomain){
			super(userId,referringDomain);
		}
		
		public int compareTo(SessionKey other){
			int output = _1().compareTo(other._1());
			if(output == 0){
				output = _2().compareTo(other._2());
			}
			return output;
		}
		
		public boolean canEqual(Object obj){
			return obj instanceof SessionKey;
		}
		
		public boolean equals(Object obj){
			return canEqual(obj) && _1().equals(((SessionKey)obj)._1()) && _2().equals(((SessionKey)obj)._2());
		}
		
	}
	
private static class SessionPartitioner extends HashPartitioner {
		
		private static final int NUM_PARTITIONS = 6;
		
		public SessionPartitioner() { super (NUM_PARTITIONS); }	
		
		public int getPartition(Object sessionKey){
			if (sessionKey instanceof SessionKey)
				return super.getPartition( ((SessionKey) sessionKey)._2());
			else
				return 0;
		}
		
		public int numPartitions() { return NUM_PARTITIONS; }
		
		public boolean equals(Object obj){
			return obj instanceof SessionPartitioner;
		}
	}
	
	public static class SessionKeyComparator implements Comparator<SessionKey> , Serializable {

		@Override
		public int compare(SessionKey o1, SessionKey o2) {
			return o1.compareTo(o2);
		}
		
	}
	
	public static final SessionKeyComparator SESSION_KEY_COMPARATOR = new SessionKeyComparator();

	private static class Event implements Serializable, Comparable<Event>{
		Event ( String event, String timestamp){
			String eventField[] = event.split(" ", 2);
			this.eventType = eventField[0];
			this.eventSubType = eventField[1];
			this.eventTimestamp = timestamp;
		}
		
		String eventType;
		String eventSubType;
		String eventTimestamp;
		
		public String toString(){
			return "<" + eventType + ":" + eventSubType +","+eventTimestamp;
		}

		@Override
		public int compareTo(Event event) {
			return EVENT_COMPARATOR.compare(this,event);
		}
		
		@Override
		public boolean equals(Object o){
		//	return o instanceof Event && compareTo((Event) o);
			boolean a = o instanceof Event;
			int compare = compareTo((Event)o);
			boolean comp = false;
			if(compare > 0){
				comp = true;
			}
			return a && comp;
		}
		
		@Override
		public int hashCode(){
			return eventType.hashCode() - eventSubType.hashCode() - eventTimestamp.hashCode();
		}
		
	}
	
		private static class EventComparator implements Comparator<Event>{

			@Override
			public int compare(Event e1, Event e2) {
				// TODO Auto-generated method stub
				int compareValue = e1.eventTimestamp.compareTo(e2.eventTimestamp);
				
				if(compareValue == 0){
					compareValue = e1.eventType.compareTo(e2.eventType);
					if(compareValue == 0){
						compareValue = e1.eventSubType.compareTo(e2.eventSubType);
					}
				}
				return compareValue;
			}
		}
		
		public static final EventComparator EVENT_COMPARATOR = new EventComparator();
		

	}