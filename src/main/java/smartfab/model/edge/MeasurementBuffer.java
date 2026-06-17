package smartfab.model.edge;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Ginaluca Bianchi
 */
public class MeasurementBuffer implements Buffer{

    private final List<Measurement> measurements;
    private final static int        WIN_SIZE = 8;
    private final static int        WIN_OVERLAP = 4;

    public MeasurementBuffer(){
        this.measurements = new ArrayList<>();
    }

    @Override
    public synchronized void addMeasurement(Measurement m) {

        //Wait to insert the new measurement until the consumer thread will process the window contained in the buffer
        while(measurements.size() >= WIN_SIZE){
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        
        this.measurements.add(m);
        //release all blocked threads
        notifyAll();
    }

    @Override
    public synchronized List<Measurement> readAllAndClear() {
        while(measurements.size() < WIN_SIZE){
            try {
                //wait for new measurements inserted (the sliding window processor will receive a complete window)
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        var copy = new ArrayList<>(this.measurements);

        /**
         * We need to "swap" the list in order to guarantee the OVERLAP of 50% of the sliding window
         * Ex: m = [1,2,3,4,5,6,7,8]
         * When some thread des the readAllAndClear(), it will return the whole buffer
         * and the buffer will be: m = [5,6,7,8,]
         */
        this.measurements.clear();
        this.measurements.addAll(copy.subList((WIN_OVERLAP-1), (WIN_SIZE-1)));

        //Thread that clear the buffer will notify the sensor thread that will be in waiting for the insertion of the new measurememnt
        notifyAll();

        return copy;
    }

    @Override
    public synchronized void clear() {
        this.measurements.clear();
    }
    
}
