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

public class GrpcPeer implements Peer{

    private final Map<PeerInfo, PeerStub>   peers;
    private final ObserverFactory           obaserverFactory;

    public GrpcPeer(){
        this.peers              = new HashMap<>();
        this.obaserverFactory   = new ObserverFactory();
    }

    public synchronized void addPeer(PeerInfo peer){
        ManagedChannel channel = ManagedChannelBuilder.forAddress(peer.getAddress(), peer.getPort())
                .usePlaintext()
                .build();
        //In this scenario we have to instantiate an async communication
        var asyncStub = CalibrationServiceGrpc.newStub(channel);
        this.peers.put(peer, new PeerStub(channel, asyncStub));
        System.out.println("PEER CONNECTIONS:");
        this.peers.keySet()
                .stream()
                .map((pi)-> pi.getID())
                .forEach(System.out::println);
    }

    public synchronized void removePeer(int peerId){
        throw new UnsupportedOperationException("PEER REMOVAL NOT SUPPORTED YET!");
    }

    @Override
    public synchronized List<PeerInfo> getAllPeers() {
        return this.peers.keySet()
                .stream()
                .collect(Collectors.toList());
    }

    @Override
    public synchronized List<PeerStub> getAllPeersStub(){
        return this.peers.values()
                .stream()
                .collect(Collectors.toList());
    }

    @Override
    public synchronized void sendRequestToAll(CalibrationRequest request) {
        new Thread(()->{
            this.peers.values()
                    .stream()
                    .forEach((peerStub) -> peerStub.getStub()
                            .requestCalibration(request, obaserverFactory.emptyStreamObserver()));
        }).start();
    }

    @Override
    public synchronized void sendGrant(CalibrationRequest req, int receiverId) {
        if(this.getStubById(receiverId).isEmpty()){
            throw new IllegalStateException("The Peer was not able to send a grpc grant message to line id: "+ receiverId);
        }

        var targetLineStub = this.getStubById(receiverId).get();

        new Thread(()->{
            targetLineStub.getStub().grantCalibrationAccess(req, this.obaserverFactory.emptyStreamObserver());
        }).start();
    }

    @Override
    public synchronized void sendReleaseToAll(int myId) {
        new Thread(()->{
                this.peers.values()
                        .stream()
                        .forEach((peerStub) -> peerStub.getStub().grantCalibrationAccess(CalibrationRequest.newBuilder()
                                .setLineId(myId)
                                .setPriority(0)
                                .build(), obaserverFactory.emptyStreamObserver()));
        }).start();
    }

    private synchronized Optional<PeerStub> getStubById(int id){
        return this.peers.entrySet().stream()
                .filter((e) -> e.getKey().getID() == id)
                .map((e) -> e.getValue())
                .findFirst();
    }

    @Override
    public synchronized void sendRequest(CalibrationRequest request, int receiverId) {
        if(this.getStubById(receiverId).isEmpty()){
            throw new IllegalStateException("The Peer was not able to send a grpc grant message to line id: "+ request.getLineId());
        }

        var targetLineStub = this.getStubById(request.getLineId()).get();
        
        new Thread(()->{
                targetLineStub.getStub().requestCalibration(request, this.obaserverFactory.emptyStreamObserver());
        }).start();

    }

    @Override
    public synchronized void sendJoinRequestToAll(PeerInfo peer) {
        this.getAllPeersStub().forEach((ps)->{
            ps.getStub().joinP2P(P2PJoinRequest.newBuilder()
                    .setLineId(peer.getID())
                    .setSnederAddress(peer.getAddress())
                    .setSenderPort(peer.getPort())
                    .build(), this.obaserverFactory.emptyStreamObserver());
        });
    }
}
