package mqtt.sino;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.logging.Logger;
import org.eclipse.paho.client.mqttv3.logging.LoggerFactory;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;

public class MqttRec{

	private MqttClient client;
	private String 				brokerUrl;
	private MqttConnectOptions 	conOpt;
	private boolean 			clean;
	private String password;
	private String userName;

	private static final String CLASS_NAME = MqttRec.class.getName();
	private static final Logger logger = LoggerFactory.getLogger(LoggerFactory.MQTT_CLIENT_MSG_CAT, CLASS_NAME);
	
	
	public static void main(String[] args) {
		
		int port 			= 1883;
		String broker 		= "10.144.128.10";
		String protocol = "tcp://";
		String clientId 	= MqttClient.generateClientId ();
		String topic 		= "lee";
		String password = null;
		String userName = null;
		boolean cleanSession = false;		// Non durable subscriptions
		int qos 			= 1;
		
		System.setProperty("java.util.logging.config.file", "D:\\workspace\\sino\\sino\\jsr47min.properties");
        System.setProperty("user.home", "D:\\log");
		
		//System.setProperty("java.util.logging.config.file", "./jsr47min.properties");
        //System.setProperty("user.home", "./log");
          
        
		String url = protocol + broker + ":" + port;
		
		try {
			MqttRec cli = new MqttRec(url, clientId, cleanSession, userName,password);
		
			cli.subscribe(topic,qos);
		
		}catch(MqttException me) {
			logger.severe("MqttRec","main","reason: "+me.getReasonCode());
			logger.severe("MqttRec","main","msg: "+me.getMessage());
			logger.severe("MqttRec","main","loc: "+me.getLocalizedMessage());
			logger.severe("MqttRec","main","cause: "+me.getCause());
			logger.severe("MqttRec","main","excep: "+me);
			me.printStackTrace();
		} catch (Throwable th) {
			logger.severe("MqttRec","main","Throwable caught "+th);
			th.printStackTrace();
		}
		
		
	}
	
	
	
	/**
	 * @param brokerUrl
	 * @param clientId
	 * @param cleanSession
	 * @param userName
	 * @param password
	 */
	public MqttRec(String brokerUrl, String clientId, boolean cleanSession,
    		String userName, String password) {
		
		this.brokerUrl = brokerUrl;
    	this.clean 	   = cleanSession;
    	this.password = password;
    	this.userName = userName;
		

		logger.info("MqttRec", "Init", "MqttRec Init...");
		
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

			client = new MqttClient(this.brokerUrl,clientId, dataStore);
			
			client.setCallback(new RecCallBack(client));

		}catch(MqttException e) {
			logger.severe("MqttRec", "MqttRec", e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	
	
	
	
	/**
	 * @param topicName
	 * @param qos
	 * @throws Throwable
	 */
	public void subscribe(String topicName, int qos) throws Throwable {
		client.connect(conOpt);
    	logger.info("MqttRec","subscribe","Connected to "+brokerUrl+" with client ID "+client.getClientId());
    	client.subscribe(topicName, qos);
    	logger.info("MqttRec","subscribe","Subscribing to topic \""+topicName+"\" qos "+qos);
	}
	

}
