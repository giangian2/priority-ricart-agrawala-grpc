package smartfab.algorithms.ricart;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Gianluca Bianchi
 *
 *      Peers whose calibration request was deferred because the local peer had
 *      priority. On release every deferred peer is granted access.
 *
 *      Kept separate from {@link RoundState} because its lifetime is different:
 *      a deferred entry outlives the round it was created in, and the round it
 *      stores is the REQUESTER's round, not ours.
 *
 *      NOT thread-safe: owned by {@link RicartEngine}.
 */
final class DeferredGrants {

    /** peerId -> the round of THAT peer's deferred request */
    private final Map<Integer,Integer> queue = new HashMap<>();

    void defer(int peerId, int round) {
        this.queue.put(peerId, round);
    }

    void remove(int peerId) {
        this.queue.remove(peerId);
    }

    /**
     * @return every deferred entry, leaving the queue empty
     */
    Map<Integer,Integer> drain() {
        Map<Integer,Integer> copy = Map.copyOf(this.queue);
        this.queue.clear();
        return copy;
    }

    void clear() {
        this.queue.clear();
    }

    boolean isEmpty() {
        return this.queue.isEmpty();
    }
}
