package smartfab.http.respository;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Repository;

import smartfab.model.edge.Measurement;

@Repository
public class LinesMeasurementRepository extends DynamicLockRepository<Integer, List<Measurement>> {

    @Override
    protected void addElement(Integer key, List<Measurement> value) {
        this.storage.computeIfAbsent(key, k -> new ArrayList<>())
                    .addAll(value);
    }
}