package smartfab.algorithms.ricart;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Gianluca Bianchi
 *
 *      The calibration request the peer is currently competing for, together
 *      with the grants collected for it.
 *
 *      Merges what used to be RequestContext and GrantTracker: same lifetime,
 *      same owner, same lock. Keeping them apart only bought two redundant
 *      monitors, since both were always reached with the engine monitor
 *      already held.
 *
 *      NOT thread-safe: owned by {@link RicartEngine}.
 */
final class RoundState {

    private int    round       = 0;
    private double criticality = 0;

    private final Set<Integer> grantsReceived = new HashSet<>();

    /**
     * Starts a brand new competition: bumps the round and drops the grants
     * collected for the previous one.
     */
    void openNewRound(double criticality) {
        this.round++;
        this.criticality = criticality;
        this.grantsReceived.clear();
    }

    int round() {
        return this.round;
    }

    double criticality() {
        return this.criticality;
    }

    void recordGrant(int peerId) {
        this.grantsReceived.add(peerId);
    }

    void forgetGrant(int peerId) {
        this.grantsReceived.remove(peerId);
    }

    void clearGrants() {
        this.grantsReceived.clear();
    }

    /**
     * @param otherPeers number of OTHER peers in the network, self excluded
     * @return true when every other peer has granted the current round
     */
    boolean hasQuorum(int otherPeers) {
        return this.grantsReceived.size() >= otherPeers;
    }
}
