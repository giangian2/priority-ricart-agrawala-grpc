package smartfab.model.edge;

import io.grpc.ManagedChannel;
import smartfab.CalibrationServiceGrpc.CalibrationServiceStub;

/**
 * @author Gianluca Bianchi
 *
 *      The gRPC channel towards one peer, together with its async stub.
 *
 *      Package private on purpose: this used to be PeerStub, exposed to the
 *      algorithm through Peer.getAllPeersStub(). Nothing outside the transport
 *      adapter has any business holding a ManagedChannel.
 */
final class PeerChannel {

    private final ManagedChannel            channel;
    private final CalibrationServiceStub    stub;

    /**
     * Guards the send path. Harmless with unary RPCs, required once messages
     * travel on a shared StreamObserver, which is not thread-safe.
     */
    private final Object sendLock = new Object();

    PeerChannel(ManagedChannel channel, CalibrationServiceStub stub) {
        this.channel    = channel;
        this.stub       = stub;
    }

    CalibrationServiceStub stub() {
        return this.stub;
    }

    Object sendLock() {
        return this.sendLock;
    }

    void shutdown() {
        this.channel.shutdown();
    }

    void shutdownNow() {
        this.channel.shutdownNow();
    }
}
