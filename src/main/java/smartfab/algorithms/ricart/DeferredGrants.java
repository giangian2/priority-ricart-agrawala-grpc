package smartfab.algorithms.ricart;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.function.IntConsumer;

/**
 * Queue of peers whose calibration requests have been deferred because the
 * local peer had priority. On release every deferred peer is granted access.
 */
final class DeferredGrants {

    private final Deque<Integer> queue = new ArrayDeque<>();

    void defer(int peerId) {
        this.queue.add(peerId);
    }

    /**
     * Sends a grant to every deferred peer (in FIFO order) and empties the queue.
     */
    void releaseAll(IntConsumer grantSender) {
        while (!this.queue.isEmpty()) {
            grantSender.accept(this.queue.poll());
        }
    }

    void clear() {
        this.queue.clear();
    }
}