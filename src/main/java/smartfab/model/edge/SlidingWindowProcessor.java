package smartfab.model.edge;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class SlidingWindowProcessor extends Thread {

    private final static int WIN_SIZE = 8;
    private final static int WIN_OVERLAP = 4;

    private final Queue<Measurement> pending;
    private final Buffer rawBuffer;
    private final AveragesBuffer averagesBuffer;
    private volatile boolean stopped;
    private final Object pauseLock;

    public SlidingWindowProcessor(Buffer rawBuffer, AveragesBuffer averagesBuffer) {
        this.rawBuffer = rawBuffer;
        this.averagesBuffer = averagesBuffer;
        this.pending = new LinkedList<>();
        this.stopped = false;
        this.pauseLock = new Object();
    }

    public void stopProcessing() {
        synchronized (pauseLock) {
            stopped = true;
        }

    }

    public void startProcessing() {
        synchronized (pauseLock) {
            stopped = false;
            pauseLock.notifyAll();
        }
        
        if (getState() == State.NEW){
            this.start();
        }
    }

    @Override
    public void run() {

        while (true) {

            synchronized (pauseLock) {
                while (stopped) {
                    try {
                        pauseLock.wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }

            List<Measurement> newMeasures = rawBuffer.readAllAndClear();

            System.out.println("Read measures: "+newMeasures.size());
            pending.addAll(newMeasures);

            while (pending.size() >= WIN_SIZE) {
                // Estrae una finestra di WIN_SIZE
                List<Measurement> window = new LinkedList<>();
                for (int i = 0; i < WIN_SIZE; i++) {
                    window.add(pending.poll());
                }
                double avg = window.stream()
                        .mapToDouble((w) -> w.value())
                        .average()
                        .orElse(0.0);
                averagesBuffer.addAverage(avg);

                List<Measurement> overlapPart = window.subList(WIN_SIZE - WIN_OVERLAP, WIN_SIZE);
                pending.addAll(overlapPart);
            }
        }
    }

    public void reset() {
        synchronized(pending){
            pending.clear();
        }
    }
}