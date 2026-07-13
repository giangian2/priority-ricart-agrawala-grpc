package smartfab.model.edge;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Gianluca Bianchi
 * 
 *      This buffer is used to store the sliding windows that the 
 *      {@link smartfab.model.edge.SlidingWindowProcessor} has already processed.
 *      It will not implement the "BUSY WAITING" pattern: the consumer
 *      thread of this buffer will be in particulary: {@link smartfab.model.edge.AveragesConsumer}
 *      that will not "busy-wait" an average to be pushed into the the buffer, but it
 *      consumes it every 10 seconds without blockng calls.
 */
public class AveragesBuffer {
    private final List<Average> averages = new ArrayList<>();

    /**
     * @param avg
     */
    public synchronized void addAverage(Average avg) {
        averages.add(avg);
        System.out.println("Added average: "+avg);
    }

    /**
     * @return all the pending averages in the list
     */
    public synchronized List<Average> readAllAndClear() {
        List<Average> copy = new ArrayList<>(averages);
        averages.clear();
        return copy;
    }

    public record Average(double value, long timestamp) {
    }
}