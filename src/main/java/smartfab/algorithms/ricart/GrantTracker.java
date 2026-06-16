package smartfab.algorithms.ricart;

import java.util.HashSet;
import java.util.Set;

/**
 * This class tracks the grants collected for the current request and decides whether the
 * quorum has been reached. This is the single place where the Ricart-Agrawala
 * quorum rule lives.
 */
final class GrantTracker {

    private final Set<Integer> acked = new HashSet<>();

    void reset() {
        this.acked.clear();
    }

    void record(int peerId) {
        this.acked.add(peerId);
    }

    int count() {
        return this.acked.size();
    }

    /**
     * @param requiredGrants number of OTHER peers (self excluded)
     * @return true when grants from all the other peers have been collected
     */
    boolean hasQuorum(int requiredGrants) {
        return this.acked.size() >= requiredGrants;
    }
}
