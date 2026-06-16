package smartfab.algorithms.ricart;

/**
 * View of the peer that the {@link PeerState} implementations are
 * allowed to drive. Keeps the state objects decoupled from the full peer API
 * (transport, dispatcher, threading) and from each other.
 */
interface RicartContext {

    int peerId();

    /** Criticality of the request the peer is currently competing for. */
    double currentCriticality();

    /** Number of other peers (self excluded) currently in the network. */
    int otherPeerCount();

    /** Immediately grant calibration access to the given peer. */
    void grantTo(int targetPeerId);

    /** Defer the given peer's request until the local peer releases. */
    void deferGrant(int targetPeerId);

    /** Record that the given peer granted access for the current request. */
    void recordGrant(int fromPeerId);

    /** @return true when grants from all other peers have been collected. */
    boolean hasFullQuorum();

    /** Transition to CALIBRATING and notify listeners that the CS is acquired. */
    void enterCriticalSection(int triggeringPeerId);
}
