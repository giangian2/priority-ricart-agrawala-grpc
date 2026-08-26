package smartfab.http.respository;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

public abstract class GlobalLockRepository<K, V> {
    protected final Map<K, V> storage   = new HashMap<>();

    public synchronized void save(K key, V value) {
        storage.put(key, value);
    }

    /**
     * ATOMIC read-then-write: returns the snapshot of the keys as they were
     * BEFORE inserting the new entry, and inserts it, under a single
     * acquisition of the monitor.
     *
     * Splitting this into findAll() + save() is NOT equivalent: two concurrent
     * callers could both read a snapshot that excludes the other, and each
     * would believe it is alone in the network.
     *
     * @return the keys present before the insertion
     */
    public synchronized List<K> saveAndSnapshot(K key, V value) {
        List<K> before = List.copyOf(storage.keySet());
        storage.put(key, value);
        return before;
    }

    public synchronized Optional<Entry<K,V>> findById(K key) {
        return this.storage.entrySet()
                .stream()
                .filter((entry)->entry.getKey().equals(key))
                .findFirst();
    }

    /**
     * Returns a defensive copy: handing out the live map would let callers
     * read it (or mutate it) outside the monitor.
     *
     * @return
     */
    public synchronized Map<K,V> findAll() {
        return Map.copyOf(this.storage);
    }

    public synchronized void deleteById(K key) {
        storage.remove(key);
    }

    public synchronized int count() {
        return storage.size();
    }
}
