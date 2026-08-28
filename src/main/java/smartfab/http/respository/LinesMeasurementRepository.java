package smartfab.http.respository;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.stereotype.Repository;

import smartfab.model.mqtt.AverageMessage;

@Repository
public class LinesMeasurementRepository extends DynamicLockRepository<Integer, List<AverageMessage>> {

    /**
     * Called by save() with the WRITE lock of the line already held, which is
     * what makes the append safe: nobody is walking this list right now.
     */
    @Override
    protected void addElement(Integer key, List<AverageMessage> value) {
        this.entryOrCreate(key, ArrayList::new).addAll(value);
    }

    /**
     * Read-only: takes the SHARED lock of the line, so several dashboards
     * querying the same production line are served at the same time. They are
     * still excluded from the MQTT thread appending new averages to that line,
     * which is what stops the stream from walking a list being mutated.
     *
     * Note where the work happens: the storage monitor is held only long
     * enough to look the list up, and the filtering — the expensive part —
     * runs under the read lock alone, where other readers are welcome.
     */
    public List<AverageMessage> findByLineIdAndWindow(int lineId, long from, long to) {
        return this.readLocked(lineId, () -> {
            List<AverageMessage> line = this.readEntry(lineId);
            if (line == null) {
                return Collections.<AverageMessage>emptyList();
            }
            return line.stream()
                    .filter((avg) -> (from == 0 && to == 0) ||
                            (avg.getTimestamp() >= from && avg.getTimestamp() <= to))
                    .collect(Collectors.toList());
        });
    }
}
