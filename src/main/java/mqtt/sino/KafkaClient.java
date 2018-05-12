package mqtt.sino;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaClient {

	 Properties props;
	 Producer<String, String> producer;
	 String topic;

	 
	 
	 
	 
	 public KafkaClient() {
		 props = new Properties();
		 
		 
		 //props.put("bootstrap.servers", "10.26.2.22:9092");
		 props.put("bootstrap.servers", "10.144.128.10:9092");  
		 //props.put("acks", "all");
		 //props.put("retries", 0);//消息失败重试
		 props.put("batch.size", 16384);
		 props.put("linger.ms", 10);
		 props.put("buffer.memory", 33554432);//消息队列大小
		 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 
		 producer = new KafkaProducer<String, String>(props);
	 }
	 
	 
	 
	 
	 public int produce(String topic, String key, String value) {
		 //System.out.println("kafka: "+topic+", "+key+ ", "+value);
		 Future<RecordMetadata> res =producer.send(new ProducerRecord<String, String>(topic, key, value));

		 return 0;
	 }
	 
	 public void close() {
		 System.out.println("close kafka!");
		 producer.close();
	 }
	
}
