package smartfab.model.edge;

import java.util.List;

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
            paused = true;
        }
         
        if (getState() == State.NEW){
            this.start();
        }
    }

    public void stopConsuming(){
        synchronized(pauseLock){
            paused = false;
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
            
                Thread.sleep(INTERVAL_MS);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
