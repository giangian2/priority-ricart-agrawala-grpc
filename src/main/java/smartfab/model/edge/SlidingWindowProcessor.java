package smartfab.model.edge;

import java.util.List;

import smartfab.model.events.CriticalStatusEvent;
import smartfab.model.events.EventDispatcher;
import smartfab.model.events.EventListener;
import smartfab.model.events.ProductionLineEvent;

/**
 * @author Gianluca Bianhci
 * 
 *         This processor has the purpose to read from the
 *         {@link smartfab.model.edge.MeasurementBuffer} any
 *         new window, compute the averegage and push the new meeasurement to
 *         the {@link smartfab.model.edge.AveragesBuffer}.
 *         It use the busy-waiting pattern
 */
public class SlidingWindowProcessor extends Thread {

    private static final double THRESHOLD = 80.0;

    private final Buffer rawBuffer;
    private final AveragesBuffer averagesBuffer;
    private volatile boolean stopped;
    private final Object pauseLock;

    private final EventDispatcher<ProductionLineEvent> dispatcher = new EventDispatcher<>();

    public SlidingWindowProcessor(Buffer rawBuffer, AveragesBuffer averagesBuffer) {
        this.rawBuffer = rawBuffer;
        this.averagesBuffer = averagesBuffer;
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

        if (getState() == State.NEW) {
            this.start();
        }
    }

    public void subscribe(EventListener<ProductionLineEvent> listener) {
        dispatcher.subscribe(listener);
    }

    private void notifyEvent(ProductionLineEvent event) {
        dispatcher.notify(event);
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

            List<Measurement> window = rawBuffer.readAllAndClear();

            System.out.println("Read measures: " + window.size());

            double avg = window.stream()
                    .mapToDouble((w) -> w.value())
                    .average()
                    .orElse(0.0);

            averagesBuffer.addAverage(avg);

            if (avg > THRESHOLD) {
                System.out.printf("[CRITICAL] ** CRITICAL SECTION ** avg = %.2f > %.0f%n", avg, THRESHOLD);
                this.notifyEvent(new CriticalStatusEvent(0, 0, 1));
            }
        }
    }
}