package KafkaTest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class KafkaTopicReader extends Thread{
	 private final ConsumerConnector consumer;
	 public static String topicName = "TesterTopic";


    public KafkaTopicReader(){
    	System.out.println("** Initialize **");
    	Properties props = new Properties();
    	props.put("zookeeper.connect","localhost:2181");
    	props.put("group.id","group_siddu_test");
    	ConsumerConfig consumerConfig = new ConsumerConfig(props);
    	consumer = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);
    }
	public static void main(String[] args) {
		System.out.println("******* Consumer Started ***************");
		KafkaTopicReader demo = new KafkaTopicReader();
		demo.start();

	}

	@Override
	public void run(){
		Map<String,Integer> topicCountMap = new HashMap<String,Integer>();
		topicCountMap.put(topicName, new Integer(1));
		Map<String,List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> stream = consumerMap.get(topicName).get(0);
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		while(it.hasNext()){
			System.out.println(new String(it.next().message()));
		}

	}

}