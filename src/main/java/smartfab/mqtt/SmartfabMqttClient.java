package smartfab.mqtt;

import java.util.concurrent.ScheduledExecutorService;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;

/**
 * @author Gianluca Bianchi
 * 
 *      We have to define a single-point class where we perform the push and listen 
 *      operations on my mqtt topics. In the current SMARTFAB context the topics
 *      will be: /smartfab/line/{id}/measurements and /smartfab/line/{id}/status
 *      In order to guarantee simplicity and cross-cutting-concerns, we are using
 *      decorator pattern in order to extend the functionalities of the 
 *      superclass {@link MqttClient}
 * 
 *      USE QOS 0 FOR MEASUREMENT
 *      USE QOS 1 FOR STATUS
 */
public class SmartfabMqttClient extends MqttClient{

    public SmartfabMqttClient(String serverURI, String clientId, MqttClientPersistence persistence,
            ScheduledExecutorService executorService) throws MqttException {
        super(serverURI, clientId, persistence, executorService);
    }

    @Override
    public void publish(String topic, MqttMessage message) throws MqttException,
			MqttPersistenceException {
		aClient.publish(topic, message, null, null).waitForCompletion(getTimeToWait());
	}
    
}
