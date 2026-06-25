package smartfab.http.respository;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class GlobalLockRepository<K, V> {
    protected final Map<K, V> storage   = new HashMap<>();

    public synchronized void save(K key, V value) {
        storage.put(key, value);
    }

    public synchronized V findById(K key) {
        return storage.get(key);
    }

    /**
     * Using a global lock on "this" the findAll operation will block the update/save
     * 
     * @return
     */
    public synchronized List<V> findAll() {
        return new ArrayList<>(storage.values());
    }

    public synchronized void deleteById(K key) {
        storage.remove(key);
    }

    public synchronized int count() {
        return storage.size();
    }
}
