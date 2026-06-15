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
import smartfab.model.edge.ObserverFactory;

public class GrpcPeer implements Peer{

    private final Map<PeerInfo, PeerStub>   peers;
    private final ObserverFactory           obaserverFactory;

    public GrpcPeer(){
        this.peers              = new HashMap<>();
        this.obaserverFactory   = new ObserverFactory();
    }

    public void addPeer(PeerInfo peer){
        ManagedChannel channel = ManagedChannelBuilder.forAddress(peer.getAddress(), peer.getPort())
        .usePlaintext()
        .build();
        //In this scenario we have to instantiate an async communication
        var asyncStub = CalibrationServiceGrpc.newStub(channel);
        this.peers.put(peer, new PeerStub(channel, asyncStub));
    }

    public void removePeer(int peerId){
        throw new UnsupportedOperationException("PEER REMOVAL NOT SUPPORTED YET!");
    }

    @Override
    public List<PeerInfo> getAllPeers() {
        return this.peers.keySet()
                .stream()
                .collect(Collectors.toList());
    }

    @Override
    public List<PeerStub> getAllPeersStub(){
        return this.peers.values()
                .stream()
                .collect(Collectors.toList());
    }

    @Override
    public void sendRequestToAll(CalibrationRequest request) {
        this.peers.values()
                .stream()
                .forEach((peerStub) -> new Thread(){
                    @Override
                    public void run(){
                        peerStub.getStub().requestCalibration(request, obaserverFactory.emptyStreamObserver());
                    }
                }.start());
    }

    @Override
    public void sendGrant(int targetLineId) {
        if(this.getStubById(targetLineId).isEmpty()){
            throw new IllegalStateException("The Peer was not able to send a grpc grant message to line id: "+targetLineId);
        }

        var targetLineStub = this.getStubById(targetLineId).get();
        targetLineStub.getStub().grantCalibrationAccess(CalibrationRequest.newBuilder()
                .setLineId(targetLineId)
                .setPriority(0)
                .build(), this.obaserverFactory.emptyStreamObserver());
    }

    @Override
    public void sendReleaseToAll(int myId) {
        this.peers.values()
                .stream()
                .forEach((peerStub) -> new Thread(){
                    @Override
                    public void run(){
                        peerStub.getStub().grantCalibrationAccess(CalibrationRequest.newBuilder()
                                .setLineId(myId)
                                .setPriority(0)
                                .build(), obaserverFactory.emptyStreamObserver());
                    }
                }.start());
    }

    private Optional<PeerStub> getStubById(int id){
        return this.peers.entrySet().stream()
                .filter((e) -> e.getKey().getID() == id)
                .map((e) -> e.getValue())
                .findFirst();
    }

    @Override
    public void sendRequest(CalibrationRequest request) {
        if(this.getStubById(request.getLineId()).isEmpty()){
            throw new IllegalStateException("The Peer was not able to send a grpc grant message to line id: "+ request.getLineId());
        }

        var targetLineStub = this.getStubById(request.getLineId()).get();
        targetLineStub.getStub().grantCalibrationAccess(request, this.obaserverFactory.emptyStreamObserver());
    }
}
