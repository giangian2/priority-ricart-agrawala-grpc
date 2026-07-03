package smartfab.model.edge;

import java.util.Date;
import java.util.List;

import smartfab.model.edge.AveragesBuffer.Average;
import smartfab.model.events.CriticalStatusEvent;
import smartfab.model.events.EventDispatcher;
import smartfab.model.events.EventListener;
import smartfab.model.events.ProductionLineEvent;

/**
 * @author Gianluca Bianhci
 * 
 *      This thired reads measurements from the
 *      {@link smartfab.model.edge.MeasurementBuffer} and
 *      computes the averegage and push the new meeasurement to
 *      the {@link smartfab.model.edge.AveragesBuffer}.
 *      BUSY-WAITING PATTERN
 */
public class SlidingWindowProcessor extends Thread {

    private static final double THRESHOLD = 80.0;

    private final Buffer            rawBuffer;
    private final AveragesBuffer    averagesBuffer;
    private volatile boolean        paused;
    private volatile boolean        stopCondition;
    private final Object            pauseLock;

    private final EventDispatcher<ProductionLineEvent> dispatcher = new EventDispatcher<>();

    public SlidingWindowProcessor(Buffer rawBuffer, AveragesBuffer averagesBuffer) {
        this.rawBuffer      = rawBuffer;
        this.averagesBuffer = averagesBuffer;
        this.paused         = false;
        this.stopCondition  = false;
        this.pauseLock      = new Object();
    }

    public void pauseProcessing() {
        synchronized (pauseLock) {
            paused = true;
            pauseLock.notifyAll();
        }

    }

    public void startProcessing() {
        synchronized (pauseLock) {
            paused = false;
            pauseLock.notifyAll();
        }

        if (getState() == State.NEW) {
            this.start();
        }
    }

    public void stopProcessing() {
        stopCondition = true;

        synchronized (pauseLock) {
            paused = false;
            pauseLock.notifyAll();
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

        while (!stopCondition) {

            synchronized (pauseLock) {
                while (paused) {
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

            averagesBuffer.addAverage(new Average(avg, new Date().getTime()));

            if (avg > THRESHOLD) {
                double criticality = (avg - THRESHOLD) / THRESHOLD;
                System.out.printf("[CRITICAL] avg = %.2f, criticality = %.3f%n", avg, criticality);
                this.notifyEvent(new CriticalStatusEvent(new Date().getTime(), criticality));
            }
        }

        this.rawBuffer.clear();
    }
}