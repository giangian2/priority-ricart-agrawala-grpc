package smartfab.http.respository;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.stereotype.Repository;

import smartfab.model.mqtt.AverageMessage;

@Repository
public class LinesMeasurementRepository extends DynamicLockRepository<Integer, List<AverageMessage>> {

    @Override
    protected void addElement(Integer key, List<AverageMessage> value) {
        this.storage.computeIfAbsent(key, k -> new ArrayList<>())
                .addAll(value);
    }

    public List<AverageMessage> findByLineIdAndWindow(int lineId, long from, long to) {
        synchronized (this.getLockFor(lineId)) {
            return this.storage.get(lineId).stream()
                    .filter((avg) -> (from == 0 && to == 0) ||
                            (avg.getTimestamp() >= from && avg.getTimestamp() <= to))
                    .collect(Collectors.toList());
        }
    }
}