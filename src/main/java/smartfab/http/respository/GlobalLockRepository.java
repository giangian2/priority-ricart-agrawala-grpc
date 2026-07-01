package smartfab.http.respository;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

public abstract class GlobalLockRepository<K, V> {
    protected final Map<K, V> storage   = new HashMap<>();

    public synchronized void save(K key, V value) {
        storage.put(key, value);
    }

    public synchronized Optional<Entry<K,V>> findById(K key) {
        return this.storage.entrySet()
                .stream()
                .filter((entry)->entry.getKey().equals(key))
                .findFirst();
    }

    /**
     * Using a global lock on "this" the findAll operation will block the update/save
     * 
     * @return
     */
    public synchronized Map<K,V> findAll() {
        return this.storage;
    }

    public synchronized void deleteById(K key) {
        storage.remove(key);
    }

    public synchronized int count() {
        return storage.size();
    }
}
