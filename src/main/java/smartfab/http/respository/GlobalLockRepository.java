package smartfab.http.respository;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public abstract class GlobalLockRepository<K, V> {
    protected final Map<K, V> storage   = new HashMap<>();

    public synchronized void save(K key, V value) {
        storage.put(key, value);
    }

    public synchronized Entry<K,V> findById(K key) {
        V val = this.storage.get(key);
        if(val == null){
            throw new IllegalStateException("ERROR 404 - Cannot find key: "+key.toString());
        }

        return new Entry<K,V>() {

            @Override
            public K getKey() {
                return key;
            }

            @Override
            public V getValue() {
                return val;
            }

            @Override
            public V setValue(V arg0) {
                throw new UnsupportedOperationException("You cannot change line status manually from result!");
            }
            
        };
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
