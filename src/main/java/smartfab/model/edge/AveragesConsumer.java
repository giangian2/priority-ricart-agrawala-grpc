package smartfab.model.edge;

import java.util.Date;
import java.util.List;

import org.eclipse.paho.client.mqttv3.MqttException;

import smartfab.model.mqtt.AverageMessage;
import smartfab.model.mqtt.MqttClientManager;

/**
 * @author Gianluca Bianchi
 * 
 * 
 * THis Thread will 
 */
public class AveragesConsumer extends Thread {

    private final AveragesBuffer    averagesBuffer;
    private final int               lineId;
    private static final int        INTERVAL_MS = 10000;
    private volatile boolean        paused = false;
    private final Object            pauseLock = new Object();

    public AveragesConsumer(int lineId, AveragesBuffer averagesBuffer) {
        this.lineId         = lineId;
        this.averagesBuffer = averagesBuffer;
    }

    public void startConsuming(){
        synchronized(pauseLock){
            paused = false;
            pauseLock.notifyAll();
        }
         
        if (getState() == State.NEW){
            this.start();
        }
    }

    public void stopConsuming(){
        synchronized(pauseLock){
            paused = true;
            pauseLock.notifyAll();
        }
    }

    @Override
    public void run() {
        while (true) {
            try {

                synchronized(pauseLock){
                    while(paused){
                        pauseLock.wait();
                    }
                }

                List<Double> medie = averagesBuffer.readAllAndClear();
                if (!medie.isEmpty()) {
                    System.out.printf("[Linea %d] Avgs computed %s%n", lineId, medie);
                }
            
                medie.stream()
                    .forEach((avg)->{
                        try {
                            System.out.println("PUSHING");
                            MqttClientManager.getInstance()
                                    .publishMeasurement(lineId, new AverageMessage(lineId, avg, new Date().getTime()));
                        } catch (MqttException e) {
                            System.out.println("FAILED TO PUSH AVG TO MQTT");
                        }
                    });
                Thread.sleep(INTERVAL_MS);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
