package smartfab.http.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import smartfab.algorithms.ricart.PeerInfo;
import smartfab.http.respository.PeerRepository;

import java.util.List;

@Service
public class PeerService {

    @Autowired
    private PeerRepository peerRepository;

    public synchronized void registerPeer(PeerInfo peer) {
        peerRepository.save(peer.getID(), peer);
    }

    public PeerInfo getPeer(int id) {
        return peerRepository.findById(id);
    }

    public List<PeerInfo> getAllPeers() {
        return peerRepository.findAll();
    }

    public void removePeer(int id) {
        peerRepository.deleteById(id);
    }
}