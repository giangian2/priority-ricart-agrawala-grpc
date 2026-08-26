package smartfab.algorithms.ricart;

import java.util.Collection;
import java.util.List;

/**
 * @author Gianluca Bianchi
 *
 *      Outbound port of the mutual exclusion algorithm: everything the
 *      algorithm needs in order to reach the other peers, expressed without a
 *      single reference to the networking library in use.
 *
 *      Deliberately absent: any accessor over the set of known peers. Who
 *      counts towards the quorum is decided by {@link PeerRegistry} inside the
 *      engine, NOT by whoever happens to hold an open channel. The transport
 *      keeps its channels private.
 */
public interface PeerTransport {

    /**
     * Opens a channel towards every candidate, sends the join request, and
     * reports which of them acknowledged it.
     *
     * A candidate becomes part of the topology ONLY after its acknowledgement:
     * a peer listed by the registry but already dead is never registered, so it
     * can never make the quorum unreachable.
     *
     * @param me            the joining peer, as the others must reach it back
     * @param candidates    peers returned by the registration server
     * @param timeoutMillis how long to wait for the acknowledgements
     */
    JoinOutcome joinNetwork(PeerInfo me, List<PeerInfo> candidates, long timeoutMillis);

    /**
     * Opens a channel towards a peer that announced itself to us. Unlike
     * joinNetwork this needs no handshake: an incoming join already means the
     * remote has committed us on its side.
     */
    void connect(PeerInfo peer);

    /**
     * Closes and forgets the channel towards the peer.
     */
    void disconnect(int peerId);

    void send(int peerId, PeerMessage message);

    void sendAll(Collection<Integer> peerIds, PeerMessage message);

    void shutdown();
}
