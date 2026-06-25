package smartfab.http.respository;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Repository;

import smartfab.model.edge.Measurement;

@Repository
public class LinesMeasurementRepository extends DynamicLockRepository<String, List<Measurement>> {

    @Override
    protected void addElement(String key, List<Measurement> value) {
        this.storage.computeIfAbsent(key, k -> new ArrayList<>())
                    .addAll(value);
    }
}