package smartfab.http.service;

import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;

import jakarta.annotation.PostConstruct;
import smartfab.model.client.MqttClientManager;
import smartfab.model.edge.AverageMessage;

@Service
public class MqttSubscriberService {
    
    private final MqttClientManager mqtt;
    private final Gson              decoder;

    public MqttSubscriberService(MqttClientManager mqtt){
        this.mqtt       = mqtt;
        this.decoder    = new Gson();
    }

    @PostConstruct
    public void init() throws MqttException{
        mqtt.subscribeAllMeasurements(new IMqttMessageListener() {

            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                var msg = decoder.fromJson(new String(message.getPayload()), AverageMessage.class);
                System.out.println("[MQTT] Received: "+msg.getAvg());
            }
            
        });
    }
}
