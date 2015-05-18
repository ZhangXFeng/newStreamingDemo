package io.transwarp.streaming.driver;


import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

import io.transwarp.streaming.business.HbaseWriter;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class MainKafkaStreaming{

	private static Log LOG = LogFactory.getLog(MainKafkaStreaming.class);
	public static Properties props = new Properties();

	static {
		URL conf = Thread.currentThread().getContextClassLoader()
				.getResource("consumer.properties");
		try {
			props.load(conf.openStream());
		} catch (IOException e) {
			LOG.error(e.getMessage(), e.getCause());
		}
	}

	public MainKafkaStreaming() {
	}

    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
    	
    	 JavaStreamingContext jssc = new JavaStreamingContext("ngmr-yarn-client", "demo", new Duration(2000), System.getenv("SPARK_HOME"), JavaStreamingContext.jarOfClass(MainKafkaStreaming.class));
		
		int numThreads = 1;
		String topicStr = props.getProperty("topic");
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		String[] topics = topicStr.split(",");
		for (String topic : topics) {
			topicMap.put(topic, numThreads);
		}
		
		String grouperId = props.getProperty("group.id");
		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("zookeeper.connect",
				props.getProperty("zookeeper.connect"));
		kafkaParams.put("group.id", grouperId);
		kafkaParams.put("auto.commit.enable", "true");
		kafkaParams.put("auto.commit.interval.ms", "1000");

		Set<String> topicSet = new HashSet<String>();
		for (String topic : topics) {
			topicSet.add(topic);
		}

		JavaPairDStream<String, String> messages = KafkaUtils.createStream(
				(JavaStreamingContext) jssc, String.class, String.class,
				StringDecoder.class, DefaultDecoder.class, kafkaParams,
				topicMap, StorageLevel.RAMFS_AND_DISK(), 3);

		JavaDStream<String> processedWords = messages
				.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, String>>, String>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<String> call(
							Iterator<Tuple2<String, String>> arg0)
							throws Exception {
						HbaseWriter hbw=new HbaseWriter();
						hbw.start();
						while(arg0.hasNext()){
							hbw.addRow(arg0.next());
						}
						return null;
					}
				});

		// Trigger the stream to start compute
		processedWords.foreachRDD(new Function2() {

			@Override
			public Object call(Object arg0, Object arg1) throws Exception {
				((JavaRDD<Object>) arg0).foreach(new VoidFunction<Object>() {

					@Override
					public void call(Object arg0) throws Exception {
					}
				});
				return arg0;
			}
		});

		jssc.start();
		jssc.awaitTermination();

    
    }
    
}
