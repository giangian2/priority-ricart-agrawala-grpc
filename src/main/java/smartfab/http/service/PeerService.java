package smartfab.http.service;

import java.util.HashMap;
import java.util.List;
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

    /**
     * Registers the new peer and returns the peers that were already registered
     * BEFORE it, in a single atomic step.
     *
     * The duplicate check and the insertion must happen under the same monitor
     * as the snapshot: otherwise two lines registering at the same instant can
     * both receive a list that excludes the other, and both would then believe
     * they are alone in the network and enter the critical section together.
     *
     * @param peerInfo the joining peer
     * @return the peers already in the network
     */
    public List<PeerInfo> registerAndList(PeerInfo peerInfo){
        synchronized (this.peerRepository) {
            if(this.peerRepository.findById(peerInfo).isPresent()){
                throw new IllegalStateException("Peer with ID= "+ peerInfo.getID() +" already exists");
            }
            return this.peerRepository.saveAndSnapshot(peerInfo, DEFAULT_STATE);
        }
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

    public Entry<PeerInfo, String> getPeer(int peerID){
        if(this.peerRepository.findById(new PeerInfo(peerID,null,0)).isEmpty()){
            throw new IllegalStateException("Error 404: Peer: "+peerID+" not found!");
        }
        return this.peerRepository.findById(new PeerInfo(peerID, null, 0)).get();
    }

    public void removePeer(PeerInfo peer){
        this.peerRepository.deleteById(peer);
    }

    public Map<PeerInfo,String> getAll(){
        return new HashMap<PeerInfo, String>(this.peerRepository.findAll());
    }
}