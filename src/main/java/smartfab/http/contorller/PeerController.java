package smartfab.http.contorller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import smartfab.algorithms.ricart.PeerInfo;
import smartfab.http.service.PeerService;

import java.util.List;

@RestController
@RequestMapping("/api/peers")
public class PeerController {

    @Autowired
    private PeerService peerService;

    @PostMapping
    public ResponseEntity<List<PeerInfo>> registerPeer(@RequestBody PeerInfo peer) {
        List<PeerInfo> updatedPeers = peerService.getAllPeers();
        peerService.registerPeer(peer);
        return ResponseEntity.ok(updatedPeers);
    }

    @GetMapping
    public ResponseEntity<List<PeerInfo>> getAllPeers() {
        return ResponseEntity.ok(peerService.getAllPeers());
    }
}