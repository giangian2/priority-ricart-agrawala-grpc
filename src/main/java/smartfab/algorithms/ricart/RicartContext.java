package smartfab.algorithms.ricart;

/**
 * @author Gianluca Bianchi
 * 
 *      View of the peer that the {@link PeerState} implementations are
 *      allowed to drive. Keeps the state objects decoupled from the full peer methos
 *      (transport, dispatcher, threading) and from each other.
 *      THIS DEISGN CHOICE IS DONE IN ORDER TO IMPROVE CROSS CUTTING CONCERNS
 */
interface RicartContext {

    /**
     * @return
     */
    int peerId();

    /**
     * 
     * @return current criticality level
     */
    double currentCriticality();

    /**
     * @return
     */
    int otherPeerCount();

    /**
     * 
     * @param targetPeerId
     * @param round
     */
    void grantTo(int targetPeerId, int round);

    /**
     * Puts the specified peer into the {@link smartfab.algorithms.ricart.DeferredGrants} queue
     * @param targetPeerId
     * @param round
     */
    void deferGrant(int targetPeerId, int round);

    /**
     * Put the specified 
     * @param fromPeerId
     */
    void recordGrant(int fromPeerId);

    /** 
     * @return true when grants from all other peers have been collected
     */
    boolean hasFullQuorum();

    /** 
     * Transition to CALIBRATING and notify listeners
     */
    void enterCriticalSection(int triggeringPeerId);

    /**
     * Restarts the last mutual exlcusion requerst process.
     * It will be consumed by {@link smartfab.algorithms.ricart.PeerState} instance
     * when the peer is in WATING state and receives a request from another peer
     * that wins the priority rule (CRITICALITY,LINE_ID) it has to restart
     * the request calibration process (after sending grant) in order to prevent
     * simultaneous calibrations due to the ASYNC nature of the system.
     */
    void restart();
}
