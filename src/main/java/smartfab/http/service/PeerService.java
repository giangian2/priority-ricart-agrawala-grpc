package smartfab.http.service;

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

    public String getPeerStatus(PeerInfo peerInfo){
        return this.peerRepository.findById(peerInfo);
    }
}