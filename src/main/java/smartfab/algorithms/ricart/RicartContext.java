package smartfab.algorithms.ricart;

/**
 * @author Gianluca Bianchi
 *
 *      The slice of the peer that {@link PeerState} implementations are allowed
 *      to drive. Keeps the state objects decoupled from the transport, the
 *      event dispatcher and each other.
 *
 *      Every method here is PURE with respect to the outside world: grantTo
 *      does not reach the network, it appends to the outbox. That is what makes
 *      the state machine testable with a hand written context, and what keeps
 *      network I/O out of the engine monitor.
 */
interface RicartContext {

    int peerId();

    /** @return the criticality of the request we are currently competing for */
    double myCriticality();

    /** @return the round of the request we are currently competing for */
    int myRound();

    /**
     * Queues a grant for the given peer. Does NOT send it.
     *
     * @param round the round of the REQUEST being granted, so the receiver can
     *              discard the grant if it belongs to a round it has abandoned
     */
    void grantTo(int targetPeerId, int round);

    /**
     * Parks the request: it will be granted when we release the section.
     */
    void deferGrant(int targetPeerId, int round);

    void recordGrant(int fromPeerId);

    /** @return true when every other peer granted the current round */
    boolean hasFullQuorum();

    /** Transitions to CALIBRATING and queues the acquisition event. */
    void enterCriticalSection(int triggeringPeerId);

    /**
     * Gives up the current competition in favour of a higher priority peer and
     * immediately competes again.
     *
     * Needed because priority here is (criticality, lineId) rather than a
     * Lamport timestamp: a peer can discover, after having already sent its own
     * request, that a competitor outranks it. Canonical Ricart-Agrawala does
     * not need this, since timestamps establish a total order known in advance.
     *
     * Opens a NEW round rather than reusing the current one: grants for the
     * abandoned attempt may still be in flight, and a fresh round number makes
     * the round check in onGrantReceived discard them without ambiguity.
     */
    void yieldAndRetry();
}
