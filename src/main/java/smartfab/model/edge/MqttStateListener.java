package smartfab.model.edge;

import org.eclipse.paho.client.mqttv3.MqttException;

import smartfab.model.events.EventListener;
import smartfab.model.events.PeerStateChangedEvent;
import smartfab.model.events.ProductionLineEvent;
import smartfab.model.mqtt.MqttClientManager;

/**
 * @author Gianluca Bianchi
 *
 *      Publishes every state transition of the algorithm over MQTT.
 *
 *      A plain EventListener like any other: the transitions travel on the same
 *      dispatcher as the calibration events, so they inherit its ordering
 *      guarantee. Publishing IDLE after CALIBRATING because two threads raced
 *      would leave the dashboard showing a state the line is not in.
 *
 *      Being a listener also keeps the MQTT singleton lookup out of the engine:
 *      with it inlined in setState the algorithm could not be instantiated
 *      without a reachable broker, and nothing in its signature said so.
 */
public final class MqttStateListener implements EventListener<ProductionLineEvent> {

    @Override
    public void onEvent(ProductionLineEvent event) {
        if (!(event instanceof PeerStateChangedEvent transition)) {
            return;
        }

        try {
            MqttClientManager.getInstance()
                    .publishNewState(transition.getPeerId(), transition.getStateName());
        } catch (MqttException e) {
            System.err.println("Cannot publish state " + transition.getStateName()
                    + " for line " + transition.getPeerId() + ": " + e.getMessage());
        }
    }
}
