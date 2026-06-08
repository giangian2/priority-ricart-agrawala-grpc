package smartfab.model.edge;

import java.util.List;

public class AveragesConsumer extends Thread {
    private final AveragesBuffer averagesBuffer;
    private final int lineId;
    private static final int INTERVAL_MS = 10000;
    private static final double THRESHOLD = 80.0;

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
                    double lastAvg = medie.get(medie.size() - 1);
                    if (lastAvg > THRESHOLD) {
                        System.out.printf("[Linea %d] ** CRITICAL SECTION ** avg = %.2f > %.0f%n",
                                lineId, lastAvg, THRESHOLD);
                        /**
                         * @todo NOTIFY THE ENGINE (PRODUCTION LINE)
                         */
                    }
                }
                Thread.sleep(INTERVAL_MS);
            } catch (InterruptedException e) {
  
                break;
            }
        }
    }
}
