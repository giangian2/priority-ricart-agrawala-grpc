package smartfab.http.respository;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Gianluca Bianchi
 * 
 *      Dynamic Lock Repository with "per-key lock".
 *      Every class that extends it will perform save
 *      and findById in parallel between different keys.
 *      If 2 or more threads will do save and find on the
 *      same key, then they will be synchronized
 */
public abstract class DynamicLockRepository<K, V>{

    protected final Map<K, V> storage = new HashMap<>();
    
    private final Map<K, Object> lockMap = new HashMap<>();

    /**
     * Retreives the lock object for a specified key.
     * If the key has no lock associated, it will be created
     * on fly. Note that all the thread will be synchronized 
     * in this block in order to manipulate the lock map
     * in a thread-safe way
     * @param key
     * @return Object lock for the key param
     */
    protected Object getLockFor(K key) {
        synchronized (lockMap) {
            if (!lockMap.containsKey(key)) {
                lockMap.put(key, new Object());
            }
            return lockMap.get(key);
        }
    }

    /**
     * 
     * @param key
     * @param value
     */
    protected abstract void addElement(K key,V value);

    public void save(K key, V value) {
        synchronized (getLockFor(key)) {
            this.addElement(key, value);
        }
    }

    public V findById(K key) {
        synchronized (getLockFor(key)) {
            return storage.get(key);
        }
    }

    /**
     * THE DELETE METHOD CAN BE EXECUTED SIMULTANEUSLY BY 
     * THREADS THAT RUNS IT ON DIFFERENT KEYS.
     * IN ORDER TO PREVENT COMPLEX DEADLOCKS AND NESTED
     * LOCKS, WE KEEP THE DELETED KEY IN THE MAP.
     * @param key
     */
    public void deleteById(K key) {
        synchronized (getLockFor(key)) {
            storage.remove(key);
        }
    }
}
