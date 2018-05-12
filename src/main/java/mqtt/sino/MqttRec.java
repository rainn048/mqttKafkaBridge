package mqtt.sino;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.logging.Logger;
import org.eclipse.paho.client.mqttv3.logging.LoggerFactory;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;

public class MqttRec{
	//MqttAsyncClient 	client;
	MqttClient client;
	String 				brokerUrl;
	private MqttConnectOptions 	conOpt;
	private boolean 			clean;
	Throwable 			ex = null;
	Object 				waiter = new Object();
	boolean 			donext = false;
	private String password;
	private String userName;

	private static final String CLASS_NAME = MqttRec.class.getName();
	private static final Logger log = LoggerFactory.getLogger(LoggerFactory.MQTT_CLIENT_MSG_CAT, CLASS_NAME);
	
	
	public static void main(String[] args) {
		
		int port 			= 1883;
		String broker 		= "10.144.128.10";
		String protocol = "tcp://";
		String clientId 	= MqttClient.generateClientId ();
		String topic 		= "lee";
		String password = "dsd";
		String userName = "sdf";
		boolean cleanSession = true;			// Non durable subscriptions
		int qos 			= 1;
		
		System.setProperty("java.util.logging.config.file", "D:\\workspace\\sino\\sino\\jsr47min.properties");
        System.setProperty("user.home", "D:\\log");
		
		//System.setProperty("java.util.logging.config.file", "./jsr47min.properties");
        //System.setProperty("user.home", "./log");
          
        log.fine("MqttRec", "main", "3");
        
		String url = protocol + broker + ":" + port;
		
		try {
			MqttRec cli = new MqttRec(url, clientId, cleanSession, userName,password);
		
			cli.subscribe(topic,qos);
		
		}catch(MqttException me) {
			System.out.println("reason "+me.getReasonCode());
			System.out.println("msg "+me.getMessage());
			System.out.println("loc "+me.getLocalizedMessage());
			System.out.println("cause "+me.getCause());
			System.out.println("excep "+me);
			me.printStackTrace();
		} catch (Throwable th) {
			System.out.println("Throwable caught "+th);
			th.printStackTrace();
		}
		
		
	}
	
	
	
	public void subscribe(String topicName, int qos) throws Throwable {


		client.connect(conOpt);

    	//log.info("MqttRec","subscribe","Connected to "+brokerUrl+" with client ID "+client.getClientId());
    	System.out.println("connect!");
    	client.subscribe(topicName, qos);
    	System.out.println("connected!");
    	//log.info("MqttRec","subscribe","Subscribing to topic \""+topicName+"\" qos "+qos);

	}
	
	
	
	public MqttRec(String brokerUrl, String clientId, boolean cleanSession,
    		String userName, String password) {
		
		this.brokerUrl = brokerUrl;
    	this.clean 	   = cleanSession;
    	this.password = password;
    	this.userName = userName;
		

		log.info("MqttRec", "Init", "asddsasd");
		
		MqttDefaultFilePersistence dataStore = new MqttDefaultFilePersistence();
		
		try {
			conOpt = new MqttConnectOptions();
			conOpt.setKeepAliveInterval(20);
			conOpt.setCleanSession(clean);
	
			if(null != password) {
				conOpt.setPassword(this.password.toCharArray());
			}
			if(null != userName) {
				conOpt.setUserName(this.userName);
			}
			//client = new MqttAsyncClient(this.brokerUrl,clientId, dataStore);
			client = new MqttClient(this.brokerUrl,clientId, dataStore);
			
			client.setCallback(new RecCallBack(client));
			
			
		}catch(MqttException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

}
