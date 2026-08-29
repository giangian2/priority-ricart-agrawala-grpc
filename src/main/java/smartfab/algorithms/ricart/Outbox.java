package smartfab.algorithms.ricart;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author Gianluca Bianchi
 *
 *      Messages the state machine decided to send, not yet handed to the
 *      transport.
 *
 *      The point of this class is that DECIDING and SENDING happen at different
 *      times: the states fill the outbox while the engine monitor is held, and
 *      {@link RicartEngine} drains it AFTER releasing the monitor. Sending
 *      while holding the lock would deadlock as soon as the transport can
 *      block, which is exactly what happens once messages travel on a
 *      persistent stream with HTTP/2 flow control.
 *
 *      WHAT DOES NOT GO THROUGH HERE: opening and closing channels.
 *      {@link PeerTransport#connect} and {@link PeerTransport#disconnect} are
 *      still called inline, under the monitor, because a channel must appear
 *      and disappear atomically with the membership change behind it. That is
 *      safe only while those two do not block — see CONNECTION LIFECYCLE in
 *      {@link RicartEngine}. The day it stops being true, this is the class
 *      they belong in: an ordered list of actions rather than of messages, so
 *      that a grant queued before a peer is dropped still goes out before the
 *      channel is closed.
 *
 *      NOT thread-safe: owned by the engine, always touched under its monitor.
 */
final class Outbox {

    record Envelope(int targetId, PeerMessage message) {}

    private final List<Envelope> pending = new ArrayList<>();

    void to(int peerId, PeerMessage message) {
        this.pending.add(new Envelope(peerId, message));
    }

    void toAll(Collection<Integer> peerIds, PeerMessage message) {
        peerIds.forEach(id -> to(id, message));
    }

    /** @return everything accumulated so far, leaving the outbox empty */
    List<Envelope> drain() {
        List<Envelope> copy = List.copyOf(this.pending);
        this.pending.clear();
        return copy;
    }
}
