package smartfab.http.respository;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Repository;

import smartfab.model.mqtt.AverageMessage;

@Repository
public class LinesMeasurementRepository extends DynamicLockRepository<Integer, List<AverageMessage>> {

    @Override
    protected void addElement(Integer key, List<AverageMessage> value) {
        this.storage.computeIfAbsent(key, k -> new ArrayList<>())
                    .addAll(value);
    }
}