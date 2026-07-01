package smartfab.model.mqtt;

import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import smartfab.util.JsonMapper;

/**
 * @author Gianluca Bianchi
 * 
 * MqttClientManager for manage mqtt client connection for both
 * Publishers and Subscribers. In this way we centralize the configuration
 * of the Client ad the publish / subscibe operations.
 */
public class MqttClientManager {

    private static final String         BROKER_URL = "tcp://localhost:1883";
    private static MqttClientManager    instance;
    private IMqttClient                 client;

    private MqttClientManager() {
        try {
            client = new MqttClient(BROKER_URL, MqttClient.generateClientId());
            MqttConnectOptions options = new MqttConnectOptions();
            options.setAutomaticReconnect(true);
            options.setCleanSession(true);
            options.setKeepAliveInterval(60);
            client.connect(options);
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    public static synchronized MqttClientManager getInstance() {
        if (instance == null) instance = new MqttClientManager();
        return instance;
    }

    private void publish(String topic, String payload) throws MqttException {
        MqttMessage message = new MqttMessage(payload.getBytes());
        message.setQos(0);
        message.setRetained(false);
        client.publish(topic, message);
    }

    private void subscribe(String topic, IMqttMessageListener listener) throws MqttException {
        client.subscribe(topic, listener);
    }

    public void publishMeasurement(int productionLine, AverageMessage measurement) throws MqttException{
        this.publish("/prodlines/"+productionLine+"/measurements",JsonMapper.serialize(measurement));
    }

    public void publishNewState(int productionLine, String newStatus) throws MqttException{
        this.publish("/prodlines/"+productionLine+"/status", JsonMapper.serialize(new ProdLineStatusMessage(productionLine, newStatus)));
    }

    public void subscribeAllMeasurements(IMqttMessageListener listener) throws MqttException{
        this.subscribe("/prodlines/+/measurements", listener);
    }

    public void subscribeAllProdLines(IMqttMessageListener listener) throws MqttException{
        this.subscribe("/prodlines/+/status", listener);
    }
}