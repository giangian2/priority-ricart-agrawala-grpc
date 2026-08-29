package smartfab.algorithms.ricart;

/**
 * @author Gianluca Bianchi
 *
 *      Messages exchanged between peers by the mutual exclusion algorithm,
 *      expressed in DOMAIN terms: no protobuf, no gRPC.
 *
 *      Keeping the wire format out of this hierarchy is what allows the
 *      algorithm to be exercised with an in-memory transport, and what makes
 *      the switch from unary RPCs to a persistent stream a change confined to
 *      {@link smartfab.model.edge.MessageCodec} and the transport adapter.
 *
 *      NOTE: the network join is NOT a PeerMessage. It is not fire-and-forget:
 *      it carries an acknowledgement, so it is a dedicated operation of
 *      {@link PeerTransport} (see PeerTransport#joinNetwork).
 */
public sealed interface PeerMessage {

    /**
     * Ask the receiver for permission to enter the calibration section.
     */
    record Request(int senderId, double criticality, int round) implements PeerMessage {}

    /**
     * Grant the sender's pending request identified by {@code round}.
     *
     * Ricart-Agrawala has no explicit release message: releasing the section
     * means flushing the deferred queue as a batch of grants.
     */
    record Grant(int senderId, int round) implements PeerMessage {}

    /**
     * The sender is voluntarily leaving the peer network.
     */
    record Leave(int senderId) implements PeerMessage {}
}
