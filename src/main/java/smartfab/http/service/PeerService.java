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

    public Entry<PeerInfo, String> getPeerStatus(int peerID){
        return this.peerRepository.findById(new PeerInfo(peerID,null,0));
    }

    public Map<PeerInfo,String> getAll(){
        return new HashMap<PeerInfo, String>(this.peerRepository.findAll());
    }
}