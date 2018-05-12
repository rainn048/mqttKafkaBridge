package mqtt.sino;


import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.logging.Logger;
import org.eclipse.paho.client.mqttv3.logging.LoggerFactory;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;

public class RecCallBack  implements MqttCallback {
	
	private MqttClient client;
	KafkaClient kafka;
	private static final String CLASS_NAME = RecCallBack.class.getName();
	private static final Logger logger = LoggerFactory.getLogger(LoggerFactory.MQTT_CLIENT_MSG_CAT, CLASS_NAME);
	
	
	public RecCallBack(MqttClient client) {
		this.client =client;
		this.kafka = new KafkaClient();
	}
	
	public void connectionLost(Throwable cause) {  
		// Called when the connection to the server has been lost.
		// An application may choose to implement reconnection
		// logic at this point. This sample simply exits.
		    // 连接丢失后，一般在这里面进行重连  
		logger.info("RecCallBack", "connectionLost", "连接断开，可以做重连");
//		    System.out.println("连接断开，可以做重连"); 
//		    System.out.println("msg"+cause.getMessage());
//		    System.out.println("reason "+((MqttException)cause).getReasonCode());
//		    System.out.println("cause "+((MqttException)cause).getCause());
//		    cause.printStackTrace();
//		    
//		    if(!client.isConnected()) {
//		    	try {
//		    		client.connect();
//		    	System.out.println("**********************");
//		    	}catch(Exception e){
//		    		System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&");
//					System.out.println("reason "+((MqttException)e).getReasonCode());
//					System.out.println("msg "+((MqttException)e).getMessage());
//					System.out.println("loc "+((MqttException)e).getLocalizedMessage());
//					System.out.println("cause "+((MqttException)e).getCause());
//					System.out.println("excep "+((MqttException)e));
//		    	}
//		    }
		    
		    kafka.close();
		}  
		
		public void deliveryComplete(IMqttDeliveryToken token) {
			// Called when a message has been delivered to the
			// server. The token passed in here is the same one
			// that was returned from the original call to publish.
			// This allows applications to perform asynchronous
			// delivery without blocking until delivery completes.
			//
			// This sample demonstrates asynchronous deliver, registering
			// a callback to be notified on each call to publish.
			//
			// The deliveryComplete method will also be called if
			// the callback is set on the client
			//
			// note that token.getTopics() returns an array so we convert to a string
			// before printing it on the console
		    System.out.println("deliveryComplete---------" + token.isComplete());  
		}
		
		public void messageArrived(String topic, MqttMessage message) throws Exception {
		    // subscribe后得到的消息会执行到这里面  
		    //System.out.print("接收消息主题 : " + topic);  
		    //System.out.print(", 接收消息Qos : " + message.getQos());  
		    //System.out.println(", 接收消息内容 : " + new String(message.getPayload()));
			logger.info("RecCallBack", "messageArrived", "接收消息主题 : " + topic+", 接收消息内容 : "+ new String(message.getPayload()));
		    kafka.produce("lee",  Integer.toString(message.getId()), new String(message.getPayload()));
		}  
		
	
}
