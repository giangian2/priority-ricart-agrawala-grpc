package smartfab.model.edge;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import smartfab.model.events.CriticalStatusEvent;
import smartfab.model.events.EventDispatcher;
import smartfab.model.events.EventListener;
import smartfab.model.events.ProductionLineEvent;


public class SlidingWindowProcessor extends Thread{

    private final static int            WIN_SIZE = 8;
    private final static int            WIN_OVERLAP = 4;
    private static final double         THRESHOLD = 80.0;

    private final Queue<Measurement>    pending;
    private final Buffer                rawBuffer;
    private final AveragesBuffer        averagesBuffer;
    private volatile boolean            stopped;
    private final Object                pauseLock;

    private final EventDispatcher<ProductionLineEvent> dispatcher = new EventDispatcher<>();

    public SlidingWindowProcessor(Buffer rawBuffer, AveragesBuffer averagesBuffer) {
        this.rawBuffer      = rawBuffer;
        this.averagesBuffer = averagesBuffer;
        this.pending        = new LinkedList<>();
        this.stopped        = false;
        this.pauseLock      = new Object();
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

   
    // Esposizione dei metodi necessari per l'esterno
    public void subscribe(EventListener<ProductionLineEvent> listener) {
        dispatcher.subscribe(listener);
    }

    // Metodo privato o protetto per emettere l'evento internamente
    private void notifyEvent(ProductionLineEvent event) {
        dispatcher.notify(event);
    }

    
    /** 
     * @TODO 
     * -Real time check if the avg THRESHOLD is reaced
     *      -Stop all the threads and reset the buffers :
     *          1){@link smartfab.model.edge.MonitoringSensor}, 
     *          2){@link smartfab.model.edge.AveragesConsumer}, 
     *          3){@link SlidingWindowProcessor}
     *      -Start the mutual exclusion peer election {@link smartfab.algorithms.ricart.RicartMutualExclusionPeer}
     */
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
                // Extract one window with size equals to WIN_SIZE
                List<Measurement> window = new LinkedList<>();
                for (int i = 0; i < WIN_SIZE; i++) {
                    window.add(pending.poll());
                }
                double avg = window.stream()
                        .mapToDouble((w) -> w.value())
                        .average()
                        .orElse(0.0);
                averagesBuffer.addAverage(avg);


                if (avg > THRESHOLD) {
                    System.out.printf("[CRITICAL] ** CRITICAL SECTION ** avg = %.2f > %.0f%n", avg, THRESHOLD);
                    
                    this.notifyEvent(new CriticalStatusEvent(0, 0, 1));
                }

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