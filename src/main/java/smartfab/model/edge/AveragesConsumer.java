package smartfab.model.edge;

import java.util.List;

public class AveragesConsumer extends Thread {
    private final AveragesBuffer averagesBuffer;
    private final int lineId;
    private static final int INTERVAL_MS = 10000;

    public AveragesConsumer(int lineId, AveragesBuffer averagesBuffer) {
        this.lineId = lineId;
        this.averagesBuffer = averagesBuffer;
    }

    @Override
    public void run() {
        while (true) {
            try {
                List<Double> medie = averagesBuffer.readAllAndClear();
                if (!medie.isEmpty()) {
                    System.out.printf("[Linea %d] Avgs computed %s%n", lineId, medie);
                    
                }
                Thread.sleep(INTERVAL_MS);
            } catch (InterruptedException e) {
  
                break;
            }
        }
    }
}
