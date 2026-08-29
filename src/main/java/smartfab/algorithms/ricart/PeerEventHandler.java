package smartfab.algorithms.ricart;

/**
 * @author Gianluca Bianchi
 *
 *      Inbound port of the mutual exclusion algorithm: everything that ARRIVES
 *      from the network.
 *
 *      This is the only interface the gRPC service implementation is allowed to
 *      depend on, so that the network layer cannot reach the application-facing
 *      commands of {@link MutualExclusionAlgorithm}.
 */
public interface PeerEventHandler {

    /**
     * @param senderId    the peer asking for the calibration section
     * @param criticality the sender's priority
     * @param round       the sender's request round
     */
    void onRequestReceived(int senderId, double criticality, int round);

    /**
     * @param senderId the peer granting access
     * @param round    the round the grant refers to: a grant for a stale round
     *                 must be discarded
     */
    void onGrantReceived(int senderId, int round);

    /**
     * A new peer announced itself. The remote has already committed us on its
     * side before sending its acknowledgement, so we register it immediately.
     */
    void onJoinPeerReceived(int senderId, String senderAddress, int senderPort);

    /**
     * The peer is leaving the network on purpose.
     */
    void onExitPeerReceived(int senderId);

    /**
     * The transport lost the connection to the peer: it is presumed crashed.
     *
     * Wired only once the persistent stream is in place (see
     * GRPC_STREAMING_DESIGN.md): with fire-and-forget unary RPCs there is no
     * connection to lose between one message and the next.
     */
    void onPeerUnreachable(int senderId);
}
