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
 *
 *      LOCKING CONTRACT, because it is not the same for every method:
 *
 *        - {@link #joinNetwork}, {@link #send} and {@link #shutdown} are called
 *          with NO lock held. They are free to block.
 *        - {@link #connect} and {@link #disconnect} are called UNDER the
 *          algorithm lock and MUST NOT BLOCK.
 *
 *      The asymmetry is not an oversight: see the two methods below.
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
     *
     * CALLED UNDER THE ALGORITHM LOCK, and therefore MUST NOT BLOCK.
     *
     * Unlike {@link #send}, which the engine defers to a flush performed after
     * releasing its monitor, this runs inside the decision itself: the channel
     * has to appear atomically with the membership change that justifies it,
     * or a message decided in the same step would find nothing to travel on.
     *
     * An implementation that must reach the network in order to connect has to
     * do it asynchronously. Blocking here stalls EVERY inbound message, since
     * they all queue on that one monitor.
     */
    void connect(PeerInfo peer);

    /**
     * Closes and forgets the channel towards the peer.
     *
     * CALLED UNDER THE ALGORITHM LOCK, and therefore MUST NOT BLOCK, for the
     * same reason as {@link #connect(PeerInfo)}. Waiting for termination, or
     * writing a goodbye on the wire, belongs to {@link #shutdown} — which is
     * called with no lock held — not here.
     */
    void disconnect(int peerId);

    void send(int peerId, PeerMessage message);

    void sendAll(Collection<Integer> peerIds, PeerMessage message);

    void shutdown();
}
