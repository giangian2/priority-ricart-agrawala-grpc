package smartfab.http.respository;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import smartfab.util.ReadWriteLock;

/**
 * @author Gianluca Bianchi
 *
 *      Dynamic Lock Repository with "per-key READ/WRITE lock".
 *
 *      Two independent degrees of parallelism:
 *
 *        1. ACROSS KEYS — every key owns its own lock, so operations on
 *           different keys never wait for each other.
 *        2. WITHIN A KEY — readers share the lock. Any number of findById()
 *           on the same key run together; a writer excludes everyone, readers
 *           and writers alike.
 *
 *      Built on {@link ReadWriteLock}, hand-written on wait()/notifyAll():
 *      nothing here comes from java.util.concurrent.
 *
 *      TWO LOCKS, TWO DIFFERENT JOBS. The per-key lock protects the MEANING of
 *      one entry — "nobody appends to this line while I am reading it". It says
 *      nothing about the map itself: two writers on two different keys hold two
 *      different locks and would still put() into the same HashMap at the same
 *      time, and a concurrent resize of a HashMap can lose entries or leave a
 *      lookup spinning. The container therefore has a lock of its own, the
 *      monitor of the storage map, held for the length of a single get/put and
 *      nothing more.
 *
 *      That is also why storage is PRIVATE: reaching it directly from a
 *      subclass is exactly the bug this design exists to prevent. Subclasses go
 *      through {@link #readEntry}, {@link #writeEntry}, {@link #entryOrCreate}
 *      and {@link #removeEntry}, which are the only places the map is touched.
 *
 *      LOCK ORDER, and it is one-way:
 *
 *          per-key ReadWriteLock  ->  lockMap monitor  ->  storage monitor
 *
 *      Nothing ever takes them in the opposite order, and none of the inner
 *      ones waits, so there is no cycle and no deadlock. Note that the outer
 *      lock is held across the whole action, the inner ones for a few
 *      instructions each: the long, possibly expensive part of a read (walking
 *      a list, filtering it) happens under the READ lock only, where other
 *      readers are welcome.
 *
 *      LOCK LIFETIME. Locks are never removed, not even by deleteById(): a key
 *      can be deleted and inserted again, and dropping its lock in between
 *      would hand two threads two different locks for the same key — which is
 *      no lock at all. The map grows with the number of DISTINCT keys ever
 *      seen: here, the number of production lines.
 *
 *      NO UPGRADE. {@link ReadWriteLock} is not reentrant and offers no way to
 *      turn a read lock into a write lock: a reader asking for the write lock
 *      would wait for its own read to finish. A subclass that needs
 *      read-then-write atomically takes the write lock from the start.
 */
public abstract class DynamicLockRepository<K, V> {

    private final Map<K, V> storage = new HashMap<>();

    private final Map<K, ReadWriteLock> lockMap = new HashMap<>();

    /**
     * Retrieves the lock pair for a key, creating it on the fly the first time
     * the key is seen. All threads synchronize here in order to manipulate the
     * lock map in a thread-safe way — briefly, and without holding any other
     * lock, so this is not the contention point it looks like.
     *
     * The atomicity that matters: two threads racing on a key nobody has used
     * yet must walk away with the SAME lock object, or they would each lock
     * their own and both enter.
     *
     * @param key
     * @return the read/write lock associated with the key
     */
    protected ReadWriteLock getLockFor(K key) {
        synchronized (this.lockMap) {
            if (!this.lockMap.containsKey(key)) {
                this.lockMap.put(key, new ReadWriteLock());
            }
            return this.lockMap.get(key);
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    //  Locking — what a subclass wraps its operations in
    // ══════════════════════════════════════════════════════════════════════

    /**
     * Runs a read-only action holding the SHARED lock of the key: concurrent
     * readers of the same key proceed together.
     *
     * The action must not modify anything. Readers do not exclude each other,
     * so two of them writing would corrupt the value with no lock ever
     * reporting a conflict.
     *
     * @param key
     * @param action what to read, returning its result
     * @return whatever the action returned
     */
    protected <T> T readLocked(K key, Supplier<T> action) {
        ReadWriteLock lock = getLockFor(key);
        lock.acquireRead();
        try {
            return action.get();
        } finally {
            lock.releaseRead();
        }
    }

    /**
     * Runs a mutating action holding the EXCLUSIVE lock of the key, shutting
     * out every other reader and writer on that same key.
     *
     * @param key
     * @param action what to modify, returning its result
     * @return whatever the action returned
     */
    protected <T> T writeLocked(K key, Supplier<T> action) {
        ReadWriteLock lock = getLockFor(key);
        lock.acquireWrite();
        try {
            return action.get();
        } finally {
            lock.releaseWrite();
        }
    }

    /** @see #writeLocked(Object, Supplier) for an action with no result */
    protected void writeLocked(K key, Runnable action) {
        writeLocked(key, () -> {
            action.run();
            return null;
        });
    }

    /*
     * The try/finally above is the reason these two methods exist at all: an
     * explicit lock, unlike synchronized, is NOT released when an exception
     * unwinds the stack. One forgotten release would leave a production line
     * locked for the rest of the run.
     */

    // ══════════════════════════════════════════════════════════════════════
    //  Storage — the only places the map is touched, all under its monitor
    // ══════════════════════════════════════════════════════════════════════

    /** @return the value of the key, null if absent */
    protected V readEntry(K key) {
        synchronized (this.storage) {
            return this.storage.get(key);
        }
    }

    protected void writeEntry(K key, V value) {
        synchronized (this.storage) {
            this.storage.put(key, value);
        }
    }

    /**
     * Reads the value of the key, inserting the one built by the factory if
     * the key has none yet.
     *
     * Read-then-write in ONE acquisition of the storage monitor. That single
     * acquisition is there for the MAP, not for the entry: the entry is
     * already serialized by the write lock of its own key. What must not
     * happen is the get() and the put() interleaving with another key's put()
     * resizing the table underneath them.
     *
     * @param key
     * @param factory builds the initial value, called only when needed
     * @return the value now associated with the key, never null
     */
    protected V entryOrCreate(K key, Supplier<V> factory) {
        synchronized (this.storage) {
            V current = this.storage.get(key);
            if (current == null) {
                current = factory.get();
                this.storage.put(key, current);
            }
            return current;
        }
    }

    protected void removeEntry(K key) {
        synchronized (this.storage) {
            this.storage.remove(key);
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    //  Repository API
    // ══════════════════════════════════════════════════════════════════════

    /**
     * Called with the WRITE lock of the key already held.
     *
     * @param key
     * @param value
     */
    protected abstract void addElement(K key, V value);

    public void save(K key, V value) {
        writeLocked(key, () -> this.addElement(key, value));
    }

    public V findById(K key) {
        return readLocked(key, () -> this.readEntry(key));
    }

    /**
     * THE DELETE METHOD CAN BE EXECUTED SIMULTANEOUSLY BY
     * THREADS THAT RUN IT ON DIFFERENT KEYS.
     * IN ORDER TO PREVENT COMPLEX DEADLOCKS AND NESTED
     * LOCKS, WE KEEP THE DELETED KEY IN THE MAP.
     * @param key
     */
    public void deleteById(K key) {
        writeLocked(key, () -> this.removeEntry(key));
    }
}
