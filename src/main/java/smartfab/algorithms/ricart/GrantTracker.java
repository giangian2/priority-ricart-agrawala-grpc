package smartfab.algorithms.ricart;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Gianluca Bianchi
 * 
 *      THREAD-SAFE grant tracker for caching ACKED calibration requests
 */
final class GrantTracker {

    private final Set<Integer>  acked;

    public GrantTracker(){
        this.acked      = new HashSet<>();
    }

    /**
     * Clears the received grants cache
     */
    public synchronized void reset() {
        this.acked.clear();
    }

    public synchronized void forget(int peerId){
        this.acked.remove(peerId);
    }

    /**
     * 
     * @param peerId
     */
    public synchronized void record(int peerId) {
        this.acked.add(peerId);
    }

    /**
     * 
     * @return
     */
    public synchronized int count() {
        return this.acked.size();
    }

    /**
     * @param requiredGrants number of OTHER peers (self excluded)
     * @return true when grants from all the other peers have been collected
     */
    public synchronized boolean hasQuorum(int requiredGrants) {
        return this.acked.size() >= requiredGrants;
    }
}
