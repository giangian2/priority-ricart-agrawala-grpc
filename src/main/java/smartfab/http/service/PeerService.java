package smartfab.http.service;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


import smartfab.algorithms.ricart.PeerInfo;
import smartfab.http.respository.PeerRepository;

@Service
public class PeerService {

    private static final String DEFAULT_STATE = "IDLE";

    @Autowired
    private PeerRepository peerRepository;

    public void registerPeer(PeerInfo peerInfo){
        this.peerRepository.save(peerInfo, DEFAULT_STATE);
    }

    public void setPeerStatus(PeerInfo peerInfo, String status){
        /**
         * The status updated is done when the PeerInfo already
         * exsists in repository. Without this guard the Map method
         * "save" will create the key if it not exsists.
         */
        if(this.peerRepository.findById(peerInfo).isPresent()){
            this.peerRepository.save(peerInfo, status);
        }
    }

    public Entry<PeerInfo, String> getPeerStatus(int peerID){
        if(this.peerRepository.findById(new PeerInfo(peerID,null,0)).isEmpty()){
            throw new IllegalStateException("Error 404: Peer: "+peerID+" not found!");
        }
        return this.peerRepository.findById(new PeerInfo(peerID, null, 0)).get();
    }

    public Map<PeerInfo,String> getAll(){
        return new HashMap<PeerInfo, String>(this.peerRepository.findAll());
    }
}