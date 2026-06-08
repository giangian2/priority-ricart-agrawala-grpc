package smartfab.model.edge;

import java.util.ArrayList;
import java.util.List;

public class AveragesBuffer {
    private final List<Double> averages = new ArrayList<>();

    public synchronized void addAverage(double avg) {
        averages.add(avg);
        System.out.println("Added average: "+avg);
    }

    public synchronized List<Double> readAllAndClear() {
        List<Double> copy = new ArrayList<>(averages);
        averages.clear();
        return copy;
    }
}