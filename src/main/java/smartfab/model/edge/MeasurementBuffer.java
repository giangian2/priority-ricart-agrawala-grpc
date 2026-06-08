package smartfab.model.edge;

import java.util.ArrayList;
import java.util.List;

public class MeasurementBuffer implements Buffer{

    private final List<Measurement> measurements;

    public MeasurementBuffer(){
        this.measurements = new ArrayList<>();
    }

    @Override
    public synchronized void addMeasurement(Measurement m) {
        this.measurements.add(m);
        notifyAll();
        System.out.println("Added measurement "+m.value());
    }

    @Override
    public synchronized List<Measurement> readAllAndClear() {
        while(measurements.size() == 0){
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        var copy = new ArrayList<>(this.measurements);
        this.measurements.clear();
        return copy;
    }

    @Override
    public synchronized void clear() {
        this.measurements.clear();
    }
    
}
