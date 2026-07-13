package smartfab.model.edge;

import java.util.List;

public interface Buffer {

    void addMeasurement(Measurement m);

    List<Measurement> readAllAndClear();

    void clear();
}
