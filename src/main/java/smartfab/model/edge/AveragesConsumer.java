package smartfab.model.edge;


import org.eclipse.paho.client.mqttv3.MqttException;

import smartfab.model.mqtt.AverageMessage;
import smartfab.model.mqtt.MqttClientManager;

/**
 * @author Gianluca Bianchi
 * 
 */
public class AveragesConsumer extends Thread {

    private final AveragesBuffer    averagesBuffer;
    private final int               lineId;
    private static final int        INTERVAL_MS = 10000;
    private volatile boolean        stopCondition;
    private volatile boolean        paused = false;
    private final Object            pauseLock = new Object();

    public AveragesConsumer(int lineId, AveragesBuffer averagesBuffer) {
        this.lineId         = lineId;
        this.stopCondition  = false;
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

    public void stopConsuming() {
        stopCondition = true;

        synchronized (pauseLock) {
            paused = false;
            pauseLock.notifyAll();
        }
    }

    public void pauseConsuming(){
        synchronized(pauseLock){
            paused = true;
            pauseLock.notifyAll();
        }
    }

    @Override
    public void run() {
        while (!stopCondition) {
            try {

                synchronized(pauseLock){
                    while(paused){
                        pauseLock.wait();
                    }
                }

                averagesBuffer.readAllAndClear().stream()
                    .forEach((avg)->{
                        try {
                            MqttClientManager.getInstance()
                                    .publishMeasurement(lineId, new AverageMessage(lineId, avg.value(), avg.timestamp()));
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

        this.averagesBuffer.readAllAndClear();
    }
}
