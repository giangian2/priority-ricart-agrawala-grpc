package smartfab.algorithms.ricart;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * @author Gianluca Bianchi
 * 
 *      THREAD-SAFE queue of peers whose calibration requests have been deferred because the
 *      local peer had priority. On release every deferred peer is granted access
 */
final class DeferredGrants {

    private final Map<Integer,Integer> queue;

    public DeferredGrants(){
        this.queue = new HashMap<>();
    }

    /**
     * Add the peer to the deferred queue
     * @param peerId
     */
    public synchronized void defer(int peerId, int round) {
        this.queue.put(peerId, round);
    }

    /**
     * Sends a grant to every deferred peer (in FIFO order) and empties the queue
     * @param grantSender
     */
    public synchronized void releaseAll(BiConsumer<Integer,Integer> grantSender) {
        for (var key : this.queue.keySet()) {
            grantSender.accept(key, this.queue.get(key));
        }
    }

    /**
     * Clears the deferred queue
     */
    public synchronized void clear() {
        this.queue.clear();
    }
}