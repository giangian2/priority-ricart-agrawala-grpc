package smartfab.algorithms.ricart;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import smartfab.CalibrationServiceGrpc;
import smartfab.Smartfab.CalibrationRequest;
import smartfab.Smartfab.P2PJoinRequest;
import smartfab.model.edge.ObserverFactory;

/**
 * @author Gianluca Bianchi
 * 
 *      THREAD-SAFE gRPC implementetion of {@link smartfab.algorithms.ricart.Peer}
 */
public class GrpcPeer implements Peer {

    private final Map<PeerInfo, PeerStub>   peers;
    private final ObserverFactory           obaserverFactory;
    private final Object                    peersLock;

    public GrpcPeer() {
        this.peers              = new HashMap<>();
        this.obaserverFactory   = new ObserverFactory();
        this.peersLock          = new Object();
    }

    public void addPeer(PeerInfo peer) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(peer.getAddress(), peer.getPort())
                .usePlaintext()
                .build();
        // In this scenario we have to instantiate an async communication
        var asyncStub = CalibrationServiceGrpc.newStub(channel);

        synchronized(this.peersLock){
            this.peers.put(peer, new PeerStub(channel, asyncStub));
            System.out.println("PEER CONNECTIONS:");
            this.getAllPeers().stream()
                    .map((pi) -> pi.getID())
                    .forEach(System.out::println);
        }


    }

    @Override
    public void removePeer(int peerId) {
        synchronized (this.peersLock) {
            this.peers.entrySet()
                    .stream()
                    .filter(e -> e.getKey().getID() == peerId)
                    .findFirst()
                    .ifPresent(entry->{
                        entry.getValue().shutdown();
                        this.peers.remove(entry.getKey());
                        System.out.println("PEER REMOVED: " + peerId);
                    });
        }
    }

    @Override
    public void shutdown() {
        synchronized (this.peersLock) { 
            this.peers.values().forEach(PeerStub::shutdown);
            this.peers.clear();
        }
    }
 
    @Override
    public List<PeerInfo> getAllPeers() {
        synchronized(this.peersLock){
            return this.peers.keySet()
                    .stream()
                    .collect(Collectors.toList());
        }
    }

    @Override
    public List<PeerStub> getAllPeersStub() {
        synchronized(this.peersLock){
            return this.peers.values()
                    .stream()
                    .collect(Collectors.toList());
        }
    }

    @Override
    public void sendRequestToAll(CalibrationRequest request) {
        // new Thread(()->{
        this.getAllPeersStub()
                .stream()
                .forEach((peerStub) -> peerStub.getStub()
                        .requestCalibration(request, obaserverFactory.emptyStreamObserver()));
        // }).start();
    }

    @Override
    public void sendGrant(CalibrationRequest req, int receiverId) {
        var receiverStub = this.getStubById(receiverId);
        if (receiverStub.isEmpty()) {
            throw new IllegalStateException(
                    "The Peer was not able to send a grpc grant message to line id: " + receiverId);
        }
        var targetLineStub = receiverStub.get();
        // new Thread(()->{
        targetLineStub.getStub().grantCalibrationAccess(req, this.obaserverFactory.emptyStreamObserver());
        // }).start();
    }

    @Override
    public void sendReleaseToAll(int myId) {
        // new Thread(()->{
        this.getAllPeersStub()
                .stream()
                .forEach((peerStub) -> peerStub.getStub().grantCalibrationAccess(CalibrationRequest.newBuilder()
                        .setLineId(myId)
                        .setPriority(0)
                        .build(), obaserverFactory.emptyStreamObserver()));
        // }).start();
    }

    private Optional<PeerStub> getStubById(int id) {
        Optional<PeerStub> result;
        synchronized(this.peersLock){
            result = this.peers.entrySet().stream()
                .filter((e) -> e.getKey().getID() == id)
                .map((e) -> e.getValue())
                .findFirst();
        }
        return result;
    }

    @Override
    public void sendRequest(CalibrationRequest request, int receiverId) {
        var receiverStub = this.getStubById(receiverId);
        if (receiverStub.isEmpty()) {
            throw new IllegalStateException(
                    "The Peer was not able to send a grpc grant message to line id: " + request.getLineId());
        }
        var targetLineStub = receiverStub.get();
        // new Thread(()->{
        targetLineStub.getStub().requestCalibration(request, this.obaserverFactory.emptyStreamObserver());
        // }).start();
    }

    @Override
    public void sendJoinRequestToAll(PeerInfo peer) {
        this.getAllPeersStub().forEach((ps) -> {
            ps.getStub().joinP2P(P2PJoinRequest.newBuilder()
                    .setLineId(peer.getID())
                    .setSnederAddress(peer.getAddress())
                    .setSenderPort(peer.getPort())
                    .build(), this.obaserverFactory.emptyStreamObserver());
                });
    }

    @Override
    public void sendExitToAll(int peerId) {
        this.getAllPeersStub().forEach(ps ->
                ps.getStub().exitP2P(P2PJoinRequest.newBuilder()
                    .setLineId(peerId)
                    .build(), this.obaserverFactory.emptyStreamObserver()));
    }
}
