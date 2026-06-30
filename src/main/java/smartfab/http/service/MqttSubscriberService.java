package smartfab.http.service;

import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.springframework.stereotype.Service;


import jakarta.annotation.PostConstruct;
import smartfab.model.client.MqttClientManager;
import smartfab.model.edge.AverageMessage;
import smartfab.util.JsonMapper;

@Service
public class MqttSubscriberService {
    
    private final MqttClientManager mqtt;

    public MqttSubscriberService(MqttClientManager mqtt){
        this.mqtt = mqtt;
    }

    @PostConstruct
    public void init() throws MqttException{
        System.out.println("[MQTT] Subscribed succesfully");
        mqtt.subscribeAllMeasurements(new IMqttMessageListener() {

            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                var msg = JsonMapper.deserialize(new String(message.getPayload()), AverageMessage.class);
                System.out.println("[MQTT] Received: "+msg.getAvg());
            }
            
        });
    }
}
