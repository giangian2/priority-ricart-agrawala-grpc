package smartfab.model.edge;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import smartfab.CalibrationServiceGrpc;
import smartfab.Smartfab.Empty;
import smartfab.algorithms.ricart.JoinOutcome;
import smartfab.algorithms.ricart.Latch;
import smartfab.algorithms.ricart.PeerInfo;
import smartfab.algorithms.ricart.PeerMessage;
import smartfab.algorithms.ricart.PeerTransport;

/**
 * @author Gianluca Bianchi
 *
 *      THREAD-SAFE gRPC implementation of {@link PeerTransport}.
 *
 *      Holds the channels and nothing else: it does not decide who is part of
 *      the network. That is PeerRegistry's job, inside the engine.
 */
public final class GrpcPeerTransport implements PeerTransport {

    private final Map<Integer, PeerChannel> channels    = new HashMap<>();
    private final Object                    channelsLock = new Object();

    // ══════════════════════════════════════════════════════════════════════
    //  Join handshake
    // ══════════════════════════════════════════════════════════════════════

    /**
     * Opens a channel towards every candidate and sends the join request, but
     * commits the channel to the topology ONLY when the peer acknowledges.
     *
     * The acknowledgement is the completion of the unary joinP2P RPC, not a new
     * message: onCompleted() fires on the trailer carrying grpc-status 0, which
     * means the remote handler ran to the end. Since CalibrationServiceImpl
     * updates its own state BEFORE writing the response, receiving the
     * acknowledgement proves the remote already has us in its topology.
     *
     * Using onNext(Empty) instead would only prove the payload was written, and
     * an exception thrown by the remote handler would go unnoticed.
     */
    @Override
    public JoinOutcome joinNetwork(PeerInfo me, List<PeerInfo> candidates, long timeoutMillis) {
        if (candidates.isEmpty()) {
            return new JoinOutcome(List.of(), List.of(), true);
        }

        final Latch          latch     = new Latch(candidates.size());
        final Object         resultLock = new Object();
        final List<PeerInfo> confirmed  = new ArrayList<>();
        final List<PeerInfo> rejected   = new ArrayList<>();

        /* channels not yet committed: closed if the peer never acknowledges */
        final Map<Integer, PeerChannel> attempted = new HashMap<>();

        for (PeerInfo target : candidates) {
            PeerChannel attempt = openChannel(target);
            attempted.put(target.getID(), attempt);

            attempt.stub().joinP2P(MessageCodec.joinRequest(me), new StreamObserver<Empty>() {

                @Override
                public void onNext(Empty ignored) {
                    /* the payload carries nothing: the acknowledgement is onCompleted */
                }

                @Override
                public void onCompleted() {
                    commit(target.getID(), attempt);
                    synchronized (resultLock) { confirmed.add(target); }
                    latch.countDown();
                }

                @Override
                public void onError(Throwable t) {
                    /*
                     * Unreachable, or its handler failed: it never registered
                     * us, so registering it would create a peer that can never
                     * grant anything and would make the quorum unreachable.
                     */
                    System.out.println("JOIN REJECTED by line " + target.getID() + ": " + t.getMessage());
                    attempt.shutdownNow();
                    synchronized (resultLock) { rejected.add(target); }
                    latch.countDown();
                }
            });
        }

        boolean allAnswered;
        try {
            allAnswered = latch.await(timeoutMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            allAnswered = false;
        }

        /* whoever did not answer in time stays out, and its channel is closed */
        synchronized (resultLock) {
            for (PeerInfo candidate : candidates) {
                if (confirmed.contains(candidate) || rejected.contains(candidate)) {
                    continue;
                }
                System.out.println("JOIN TIMED OUT for line " + candidate.getID());
                attempted.get(candidate.getID()).shutdownNow();
                rejected.add(candidate);
            }
            return new JoinOutcome(List.copyOf(confirmed), List.copyOf(rejected), allAnswered);
        }
    }

    /**
     * An incoming join needs no handshake: the remote committed us before
     * acknowledging, so by the time we hear about it the relation already
     * holds in that direction.
     */
    @Override
    public void connect(PeerInfo peer) {
        commit(peer.getID(), openChannel(peer));
    }

    private PeerChannel openChannel(PeerInfo peer) {
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(peer.getAddress(), peer.getPort())
                .usePlaintext()
                .build();

        return new PeerChannel(channel, CalibrationServiceGrpc.newStub(channel));
    }

    private void commit(int peerId, PeerChannel channel) {
        synchronized (this.channelsLock) {
            PeerChannel previous = this.channels.put(peerId, channel);
            if (previous != null) {
                /* replacing without closing would leak the connection */
                previous.shutdown();
            }
        }
        System.out.println("PEER CONNECTED: " + peerId);
    }

    @Override
    public void disconnect(int peerId) {
        final PeerChannel removed;
        synchronized (this.channelsLock) {
            removed = this.channels.remove(peerId);
        }
        if (removed != null) {
            removed.shutdown();
            System.out.println("PEER DISCONNECTED: " + peerId);
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    //  Sending
    // ══════════════════════════════════════════════════════════════════════

    @Override
    public void send(int peerId, PeerMessage message) {
        PeerChannel target;
        synchronized (this.channelsLock) {
            target = this.channels.get(peerId);
        }

        if (target == null) {
            /*
             * The peer left between the decision and the flush. Not an error:
             * the engine has already dropped it, or is about to.
             */
            System.out.println("DROPPED message to unknown line " + peerId);
            return;
        }

        try {
            dispatch(peerId, target, message);
        } catch (RuntimeException e) {
            /*
             * A dead peer must not abort the delivery to the others: the
             * removal is driven by onExitPeerReceived / onPeerUnreachable.
             */
            System.out.println("SEND FAILED to line " + peerId + ": " + e.getMessage());
        }
    }

    /**
     * Sends the message on a Context detached from any incoming call.
     *
     * A send is almost always triggered by a RECEIVED message: a request makes
     * us send a grant. On that path the thread carries the io.grpc.Context of
     * the server call, and an outgoing RPC started there INHERITS it. The
     * moment the handler closes its own response the Context is cancelled,
     * and it takes the still in flight grant with it:
     *
     *     CANCELLED: io.grpc.Context was cancelled without error
     *
     * The peer waiting for that grant then waits forever, with nothing in any
     * log to explain it. Attaching Context.ROOT states what is actually true:
     * these messages are independent notifications, not the reply to the call
     * that happened to trigger them.
     *
     * The anonymous threads that used to wrap every handler hid this by
     * accident, since a new thread carries no Context: removing them (commit
     * 6fe42dd) is what exposed it.
     */
    private void dispatch(int peerId, PeerChannel target, PeerMessage message) {
        final Context previous = Context.ROOT.attach();
        try {
            sendUnderRootContext(peerId, target, message);
        } finally {
            Context.ROOT.detach(previous);
        }
    }

    private void sendUnderRootContext(int peerId, PeerChannel target, PeerMessage message) {
        synchronized (target.sendLock()) {
            if (message instanceof PeerMessage.Request request) {
                target.stub().requestCalibration(MessageCodec.toProto(request), replyOf(peerId, message));
            } else if (message instanceof PeerMessage.Grant grant) {
                target.stub().grantCalibrationAccess(MessageCodec.toProto(grant), replyOf(peerId, message));
            } else if (message instanceof PeerMessage.Leave leave) {
                target.stub().exitP2P(MessageCodec.toProto(leave), replyOf(peerId, message));
            } else {
                throw new IllegalArgumentException("Unsupported message: " + message);
            }
        }
    }

    @Override
    public void sendAll(Collection<Integer> peerIds, PeerMessage message) {
        peerIds.forEach(id -> send(id, message));
    }

    @Override
    public void shutdown() {
        synchronized (this.channelsLock) {
            this.channels.values().forEach(PeerChannel::shutdown);
            this.channels.clear();
        }
    }

    /**
     * The reply of every algorithm RPC is an empty message nobody reads: these
     * are one directional notifications, the answer to a request arrives later
     * as a separate incoming RPC.
     *
     * The payload is ignored, but the FAILURE is not: a silently dropped send
     * turns into a peer waiting forever for a grant that was never delivered,
     * with nothing in any log to explain it.
     */
    private static StreamObserver<Empty> replyOf(int peerId, PeerMessage sent) {
        return new StreamObserver<Empty>() {
            @Override public void onNext(Empty value) { }

            @Override public void onError(Throwable t) {
                System.out.println("SEND FAILED to line " + peerId + " [" + sent + "]: " + t.getMessage());
            }

            @Override public void onCompleted() { }
        };
    }
}
