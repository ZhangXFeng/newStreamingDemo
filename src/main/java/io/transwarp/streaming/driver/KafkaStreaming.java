package io.transwarp.streaming.driver;

import io.transwarp.streaming.business.HbaseWriter;

import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;
import spark.jobserver.SparkJob;
import spark.jobserver.SparkJobValidation;

import com.typesafe.config.Config;

public class KafkaStreaming implements SparkJob, Serializable {

	private static final long serialVersionUID = 1L;
	private static Log LOG = LogFactory.getLog(KafkaStreaming.class);
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

	public KafkaStreaming() {
	}

	static JavaStreamingContext jssc = null;

	@SuppressWarnings({ "rawtypes", "serial", "unchecked" })
	public Object runJob(SparkContext sc, Config arg1) {

		jssc = new JavaStreamingContext(new StreamingContext(sc, new Duration(
				2000)));
		
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

		return null;
	}

	@Override
	public Object stop() {
		// TODO Auto-generated method stub
		if (jssc != null) {
			jssc.stop(false);
		}
		return null;
	}

	@Override
	public SparkJobValidation validate(SparkContext arg0, Config arg1) {
		return spark.jobserver.SparkJobValid$.MODULE$;
	}

	@Override
	public HashMap<Object, Object> getJobStatus(Object arg0) {
		HashMap<String, Long> lags = jssc.getUnprocessedRecords();
		for (String executor : lags.keySet()) {
			System.out.println(executor + " : " + lags.get(executor));
		}

		HashMap<Object, Object> result = new HashMap<Object, Object>();
		for (String key : lags.keySet()) {
			result.put(key, lags.get(key) + "");
		}

		return result;
	}
}
