package io.transwarp.kafka.partitioner;

import io.transwarp.kafka.producer.KafkaProducer;
import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * 
 */
public class MyPartitioner implements Partitioner<String> {

    public MyPartitioner (VerifiableProperties props) {

    }

    public int partition(String key, int a_numPartitions) {
    	int partition=Integer.parseInt(key) % Integer.parseInt(KafkaProducer.props.getProperty("partitions.number"));
    	
    	return partition;
    	
    }

}