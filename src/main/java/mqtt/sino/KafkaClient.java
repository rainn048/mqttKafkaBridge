package mqtt.sino;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.eclipse.paho.client.mqttv3.logging.Logger;
import org.eclipse.paho.client.mqttv3.logging.LoggerFactory;

public class KafkaClient {

	 Properties props;
	 Producer<String, String> producer;
	 String topic;
	 private static final String CLASS_NAME = KafkaClient.class.getName();
		private static final Logger logger = LoggerFactory.getLogger(LoggerFactory.MQTT_CLIENT_MSG_CAT, CLASS_NAME);
		
	 
	 
	 
	 public KafkaClient() {
		 props = new Properties();
		 
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
		 Future<RecordMetadata> res =producer.send(new ProducerRecord<String, String>(topic, key, value));

		 return 0;
	 }
	 
	 public void close() {
		 logger.info("KafkaClient", "close", "close kafka!");
		 producer.close();
	 }
	
}
