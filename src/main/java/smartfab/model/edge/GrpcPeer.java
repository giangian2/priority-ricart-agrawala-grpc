package smartfab.model.edge;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import smartfab.CalibrationServiceGrpc;
import smartfab.Smartfab.CalibrationRequest;
import smartfab.algorithms.ricart.Peer;
import smartfab.algorithms.ricart.PeerInfo;

public class GrpcPeer implements Peer{

    private final Map<PeerInfo, PeerStub>   peers;
    private final ObserverFactory           obaserverFactory;

    public GrpcPeer(){
        this.peers              = new HashMap<>();
        this.obaserverFactory   = new ObserverFactory();
    }

    public void addPeer(PeerInfo peer){
        ManagedChannel channel = ManagedChannelBuilder.forAddress(peer.getAddress(), peer.getPort())
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
        targetLineStub.getStub().grantCalibrationAccess(null, this.obaserverFactory.emptyStreamObserver());
    }

    @Override
    public void sendReleaseToAll(int myId) {
        throw new UnsupportedOperationException("Unimplemented method 'sendReleaseToAll'");
    }

    private Optional<PeerStub> getStubById(int id){
        return this.peers.entrySet().stream()
                .filter((e) -> e.getKey().getID() == id)
                .map((e) -> e.getValue())
                .findFirst();
    }
    
}
