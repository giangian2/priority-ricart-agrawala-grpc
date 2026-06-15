package smartfab.model.edge;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Gianluca Bianchi
 * 
 * This buffer is used to store the sliding windows that the 
 * {@link smartfab.model.edge.SlidingWindowProcessor} has already processed.
 * This buffer will not implement the "BUSY WAITING" pattern: the consumer
 * thread of this buffer will be in particulary: {@link smartfab.model.edge.AveragesConsumer}
 * that will not "busy-wait" an average to be pushed into the the buffer, but it
 * consumes it every 10 seconds without blockng calls.
 */
public class AveragesBuffer {
    private final List<Double> averages = new ArrayList<>();

    /**
     * 
     * @param avg
     */
    public synchronized void addAverage(double avg) {
        averages.add(avg);
        System.out.println("Added average: "+avg);
    }

    /**
     * 
     * @return
     */
    public synchronized List<Double> readAllAndClear() {
        List<Double> copy = new ArrayList<>(averages);
        averages.clear();
        return copy;
    }
}