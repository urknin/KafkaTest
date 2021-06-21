package KafkaTest;

import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaTopicWriter {

	Properties props = new Properties();
	private static int numberOfRecord = 10;

	public void init() throws InterruptedException{
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("kafka.topic.name", "TesterTopic");
	    KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(this.props,new StringSerializer(), new ByteArraySerializer());

//	    Callback callback = new Callback() {
//            public void onCompletion(RecordMetadata metadata, Exception e) {
//            	//Logger logger=LoggerFactory.getLogger(KafkaTopicWriter.class); 
//                if (e != null) {
//                	
//                    e.printStackTrace();}
//                else {
//                	 
//                	System.out.println("AsynchronousProducer call Success:");
//                }
//            }
//        };

		for(int i=1;i<=numberOfRecord;i++){
		  byte[] payload = (i+" Siddart From Eclipse "+new Date()).getBytes();
	      ProducerRecord<String, byte[]> record =
	    		  new ProducerRecord<String, byte[]>(props.getProperty("kafka.topic.name"), payload);
	      //producer.send(record);
	      Thread.sleep(1000);
	      producer.send(record,new MyProducerCallback());//Callbacks for records being sent to the same partition are guaranteed to execute in order.
		}

		producer.close();

	}
	
	class MyProducerCallback implements Callback {

	    public void onCompletion(RecordMetadata metadata, Exception e) {
	        if (e != null)
	            System.out.println("AsynchronousProducer failed with an exception");
	        else
	            System.out.println("AsynchronousProducer call Success:");
	    }
	}

	public static void main(String[] args) throws InterruptedException {

		KafkaTopicWriter kafkaWrite = new KafkaTopicWriter();
		kafkaWrite.init();

	}

}