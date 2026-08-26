package smartfab.algorithms.ricart;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import smartfab.model.events.CalibrationGrantEvent;
import smartfab.model.events.EventDispatcher;
import smartfab.model.events.EventListener;
import smartfab.model.events.PeerStateChangedEvent;
import smartfab.model.events.ProductionLineEvent;

/**
 * @author Gianluca Bianchi
 *
 *      Ricart-Agrawala mutual exclusion.
 *      {@link https://en.wikipedia.org/wiki/Ricart%E2%80%93Agrawala_algorithm}
 *
 *      SINGLE POINT of the algorithm: the only owner of the mutable state and
 *      the only holder of a lock. It exposes three views of itself and nothing
 *      else:
 *
 *        - {@link MutualExclusionAlgorithm} : what the application asks (OUT)
 *        - {@link PeerEventHandler}         : what the network delivers (IN)
 *        - {@link RicartContext}            : what the states may drive
 *
 *      Every public method funnels through {@link #step(Runnable)}, which
 *      decides under the monitor and performs I/O outside of it: no message and
 *      no event ever leaves this class while the lock is held.
 *
 *      CONNECTION LIFECYCLE IS THE ONE EXCEPTION, and it is deliberate.
 *      {@link PeerTransport#connect} and {@link PeerTransport#disconnect} are
 *      called from INSIDE the monitor, in {@link #onJoinPeerReceived} and
 *      {@link #forgetPeer}. A channel has to appear and disappear atomically
 *      with the membership change that causes it: opening it after the monitor
 *      is released would let a message decided in the same step find nothing to
 *      travel on, and closing it there would race with the next decision.
 *
 *      Two properties make that safe TODAY, and neither is permanent:
 *
 *        1. The lock order is one-way. This monitor is taken first, the
 *           transport's channel lock second, and nothing in the transport ever
 *           takes this monitor back: its gRPC callbacks only log. No cycle, so
 *           no deadlock.
 *        2. Neither call reaches the network. A gRPC channel is lazy and dials
 *           on its first RPC, and ManagedChannel.shutdown() returns without
 *           waiting. Both calls are, in practice, a map insertion and a map
 *           removal.
 *
 *      PROPERTY 2 DIES WITH THE MOVE TO PERSISTENT STREAMS
 *      (GRPC_STREAMING_DESIGN.md, section 10): connect() would have to open the
 *      communicate() stream, an actual RPC, and disconnect() to write
 *      onCompleted() on it. Both would then block on HTTP/2 flow control
 *      towards a slow peer while every inbound handler queues on this monitor —
 *      which is the deadlock {@link Outbox} exists to prevent. That migration
 *      therefore has a prerequisite: route these two through the flush as well.
 */
public final class RicartEngine implements MutualExclusionAlgorithm, PeerEventHandler, RicartContext {

    private final PeerInfo                              self;
    private final int                                   peerId;
    private final PeerTransport                         transport;

    /*
     * Composed, not inherited, like SlidingWindowProcessor does: a subclass of
     * EventDispatcher would also drag Object.notify()/notifyAll() into the same
     * class that uses them for the shutdown protocol.
     */
    private final EventDispatcher<ProductionLineEvent>  dispatcher;

    private final PeerRegistry      registry;
    private final RoundState        roundState;
    private final DeferredGrants    deferred;
    private final Outbox            outbox;

    /* Side effects decided under the monitor and executed after releasing it. */
    private final List<ProductionLineEvent> pendingEvents;

    private PeerState state;
    private boolean   joined;

    public RicartEngine(PeerTransport transport, PeerInfo self) {
        this.transport      = transport;
        this.self           = self;
        this.peerId         = self.getID();
        this.dispatcher     = new EventDispatcher<>();

        this.registry       = new PeerRegistry();
        this.roundState     = new RoundState();
        this.deferred       = new DeferredGrants();
        this.outbox         = new Outbox();

        this.pendingEvents  = new ArrayList<>();

        this.state          = new IdleState();
        this.joined         = false;
    }

    // ══════════════════════════════════════════════════════════════════════
    //  SINGLE POINT
    // ══════════════════════════════════════════════════════════════════════

    /**
     * Runs a decision under the monitor, then flushes everything it produced
     * OUTSIDE of it.
     *
     * The split matters: the transport may block (HTTP/2 flow control on a
     * persistent stream), the MQTT publish is network I/O too, and the event
     * dispatcher hands control to application code. Doing any of those while
     * holding the monitor would let the gRPC receiving thread deadlock against
     * the thread that is sending.
     *
     * The split covers messages and events, NOT the opening and closing of
     * channels: see CONNECTION LIFECYCLE in the class javadoc for why those two
     * stay inside, and for what would have to change first.
     */
    private void step(Runnable decision) {
        final List<Outbox.Envelope>     toSend;
        final List<ProductionLineEvent> toPublish;

        synchronized (this) {
            decision.run();
            toSend    = this.outbox.drain();
            toPublish = List.copyOf(this.pendingEvents);
            this.pendingEvents.clear();
        }

        flush(toSend, toPublish);
    }

    private void flush(List<Outbox.Envelope> toSend, List<ProductionLineEvent> toPublish) {
        toSend.forEach(envelope -> this.transport.send(envelope.targetId(), envelope.message()));
        toPublish.forEach(this.dispatcher::publish);
    }

    /**
     * Must be called with the monitor held.
     *
     * The transition is published as an ordinary event: whoever cares (the MQTT
     * publisher) subscribes like any other listener, and gets the transitions
     * in the order they happened.
     */
    private void setState(PeerState newState) {
        this.state = newState;
        this.pendingEvents.add(new PeerStateChangedEvent(
                this.peerId, newState.name(), new Date().getTime()));
    }

    // ══════════════════════════════════════════════════════════════════════
    //  OUT — application commands
    // ══════════════════════════════════════════════════════════════════════

    @Override
    public boolean join(List<PeerInfo> fromRegistry, long timeoutMillis) {
        /*
         * The handshake blocks, so it deliberately runs OUTSIDE step(): only
         * its result is folded into the state.
         */
        JoinOutcome outcome = this.transport.joinNetwork(this.self, fromRegistry, timeoutMillis);

        step(() -> {
            this.registry.addAll(outcome.confirmed());
            this.joined = true;
        });

        System.out.println("LINE " + this.peerId + ": joined with " + outcome.confirmed().size()
                + " peer(s), " + outcome.rejected().size() + " unreachable");
        return outcome.allAnswered();
    }

    @Override
    public void requestCalibration(double criticality) {
        step(() -> doRequestCalibration(criticality));
    }

    /** Must be called with the monitor held. */
    private void doRequestCalibration(double criticality) {
        if (!this.joined) {
            System.out.println("LINE " + this.peerId + ": join not completed yet, trigger ignored");
            return;
        }

        if (this.state instanceof CalibratingState) {
            System.out.println("LINE " + this.peerId + ": already CALIBRATING, trigger ignored");
            return;
        }

        this.roundState.openNewRound(criticality);
        System.out.println("LINE " + this.peerId + ": Request Calibration, ROUND " + this.roundState.round());

        if (this.registry.size() == 0) {
            enterCriticalSection(this.peerId);
            return;
        }

        broadcastRequest();
        setState(new WaitingState());
    }

    /** Must be called with the monitor held. */
    private void broadcastRequest() {
        this.outbox.toAll(this.registry.ids(), new PeerMessage.Request(
                this.peerId, this.roundState.criticality(), this.roundState.round()));
    }

    @Override
    public void releaseCalibration() {
        step(this::doReleaseCalibration);
    }

    /** Must be called with the monitor held. */
    private void doReleaseCalibration() {
        setState(new IdleState());
        this.roundState.clearGrants();

        /*
         * Ricart-Agrawala has no release message: releasing IS the batch of
         * grants owed to everyone we deferred.
         */
        Map<Integer,Integer> owed = this.deferred.drain();
        owed.forEach((targetId, round) -> this.outbox.to(targetId, new PeerMessage.Grant(this.peerId, round)));

        /* wakes up a shutdown() waiting for the section to be released */
        notifyAll();
    }

    @Override
    public void shutdown() {
        final List<Outbox.Envelope>     toSend;
        final List<ProductionLineEvent> toPublish;

        synchronized (this) {
            while (this.state instanceof CalibratingState) {
                System.out.println("Shutdown: waiting for the calibration to end");
                try {
                    wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            if (this.state instanceof WaitingState) {
                doReleaseCalibration();
            } else {
                this.roundState.clearGrants();
                this.deferred.clear();
            }

            this.outbox.toAll(this.registry.ids(), new PeerMessage.Leave(this.peerId));
            this.joined = false;

            toSend    = this.outbox.drain();
            toPublish = List.copyOf(this.pendingEvents);
            this.pendingEvents.clear();
        }

        flush(toSend, toPublish);
        this.transport.shutdown();
        System.out.println("Shutdown completed");
    }

    @Override
    public void subscribe(EventListener<ProductionLineEvent> listener) {
        this.dispatcher.subscribe(listener);
    }

    // ══════════════════════════════════════════════════════════════════════
    //  IN — network events
    // ══════════════════════════════════════════════════════════════════════

    @Override
    public void onRequestReceived(int senderId, double criticality, int round) {
        step(() -> this.state.onRequest(this, senderId, criticality, round));
    }

    @Override
    public void onGrantReceived(int senderId, int round) {
        step(() -> {
            /*
             * A single peer lives through many rounds: a grant is only
             * meaningful for the round it was asked for. Accepting a stale one
             * would let an abandoned competition complete the quorum of the
             * current one.
             */
            if (this.roundState.round() == round) {
                this.state.onGrant(this, senderId, round);
            } else {
                System.out.println("LINE " + this.peerId + ": stale grant from " + senderId
                        + " (round " + round + ", current " + this.roundState.round() + ")");
            }
        });
    }

    @Override
    public void onJoinPeerReceived(int senderId, String senderAddress, int senderPort) {
        step(() -> {
            System.out.println("NEW PEER JOINED THE NETWORK: " + senderId
                    + " [" + senderAddress + ":" + senderPort + "]");

            PeerInfo newcomer = new PeerInfo(senderId, senderAddress, senderPort);
            this.registry.add(newcomer);

            /*
             * Under the monitor on purpose, and safe only for as long as
             * connect() does not block: CONNECTION LIFECYCLE, class javadoc.
             */
            this.transport.connect(newcomer);

            /*
             * If we are competing, the newcomer knows nothing about our pending
             * request, and it just became part of our quorum: without telling
             * it, the quorum can never be completed.
             *
             * We cannot reuse doRequestCalibration here: that would open a new
             * round and drop the grants already collected.
             */
            if (this.state instanceof WaitingState) {
                this.outbox.to(senderId, new PeerMessage.Request(
                        this.peerId, this.roundState.criticality(), this.roundState.round()));
            }
        });
    }

    @Override
    public void onExitPeerReceived(int senderId) {
        step(() -> forgetPeer(senderId, "PEER LEFT NETWORK"));
    }

    @Override
    public void onPeerUnreachable(int senderId) {
        step(() -> forgetPeer(senderId, "PEER UNREACHABLE, presumed crashed"));
    }

    /**
     * Must be called with the monitor held.
     *
     * Idempotent: a peer can be reported gone twice, e.g. an explicit leave
     * still in flight followed by the connection dropping.
     */
    private void forgetPeer(int senderId, String reason) {
        if (!this.registry.remove(senderId)) {
            return;
        }
        System.out.println(reason + ": " + senderId);

        /* under the monitor: CONNECTION LIFECYCLE, class javadoc */
        this.transport.disconnect(senderId);

        this.deferred.remove(senderId);
        this.roundState.forgetGrant(senderId);

        /*
         * Shrinking the network can complete a quorum that was one grant short.
         */
        if (this.state instanceof WaitingState && hasFullQuorum()) {
            enterCriticalSection(this.peerId);
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    //  RicartContext — driven by the states, always under the monitor
    // ══════════════════════════════════════════════════════════════════════

    @Override
    public int peerId() {
        return this.peerId;
    }

    @Override
    public double myCriticality() {
        return this.roundState.criticality();
    }

    @Override
    public int myRound() {
        return this.roundState.round();
    }

    @Override
    public void grantTo(int targetPeerId, int round) {
        this.outbox.to(targetPeerId, new PeerMessage.Grant(this.peerId, round));
    }

    @Override
    public void deferGrant(int targetPeerId, int round) {
        this.deferred.defer(targetPeerId, round);
    }

    @Override
    public void recordGrant(int fromPeerId) {
        this.roundState.recordGrant(fromPeerId);
    }

    @Override
    public boolean hasFullQuorum() {
        return this.roundState.hasQuorum(this.registry.size());
    }

    @Override
    public void enterCriticalSection(int triggeringPeerId) {
        setState(new CalibratingState());
        this.pendingEvents.add(new CalibrationGrantEvent(triggeringPeerId, new Date().getTime()));
    }

    @Override
    public void yieldAndRetry() {
        System.out.println("LINE " + this.peerId + ": yielding to a higher priority peer, competing again");
        this.roundState.openNewRound(this.roundState.criticality());
        broadcastRequest();
        setState(new WaitingState());
    }

}
