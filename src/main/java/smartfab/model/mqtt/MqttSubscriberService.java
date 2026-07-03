package smartfab.model.mqtt;

import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import smartfab.algorithms.ricart.PeerInfo;
import smartfab.http.service.MeasurementService;
import smartfab.http.service.PeerService;
import smartfab.util.JsonMapper;

@Service
public class MqttSubscriberService {
    
    private final MqttClientManager     mqtt;
    private final MeasurementService    measurementService;
    private final PeerService           peerService;

    public MqttSubscriberService(MqttClientManager mqtt, MeasurementService measurementService, PeerService peerService){
        this.mqtt               = mqtt;
        this.measurementService = measurementService;
        this.peerService        = peerService;
    }

    @PostConstruct
    public void init() throws MqttException{
        mqtt.subscribeAllMeasurements((String topic, MqttMessage message) -> {
            var msg = JsonMapper.deserialize(new String(message.getPayload()), AverageMessage.class);
            if(topic.split("/").length >= 2){
                try{
                    System.out.println("[MQTT] From line: "+ msg.getLineId()+ " received new avg: "+msg.getAvg());
                    measurementService.addMeasurement(Integer.parseInt(topic.split("/")[2]), msg);
                }catch(NumberFormatException e){
                    System.out.println("Error on topic: "+topic);
                }
            }
        });

        mqtt.subscribeAllProdLines((String topic, MqttMessage message)->{
            var msg = JsonMapper.deserialize(new String(message.getPayload()), ProdLineStatusMessage.class);
            if(topic.split("/").length >= 2){
                try{
                    System.out.println("[MQTT] New status received from line "+msg.getLineId()+": "+ msg.getStatus());
                    peerService.setPeerStatus(new PeerInfo(msg.getLineId(), null, 0), msg.getStatus());
                }catch(NumberFormatException e){
                    System.out.println("Error on topic: "+topic);
                }
            }
        });
    }
}
