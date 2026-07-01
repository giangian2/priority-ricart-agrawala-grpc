package smartfab.http.contorller;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import smartfab.algorithms.ricart.PeerInfo;
import smartfab.http.service.MeasurementService;
import smartfab.http.service.PeerService;
import smartfab.model.mqtt.AverageMessage;


@RestController
@RequestMapping("/api/prodlines")
public class ProductionLineController {

    @Autowired
    private PeerService         peerService;
    @Autowired
    private MeasurementService  measurementService;

    @PostMapping
    public ResponseEntity<List<PeerInfo>> registerProductionLine(@RequestBody PeerInfo peer) {
        Map<PeerInfo,String> updatedPeers = peerService.getAll();
        peerService.registerPeer(peer);
        return ResponseEntity.ok(new ArrayList<>(updatedPeers.keySet()));
    }

    @GetMapping
    public ResponseEntity<Map<PeerInfo,String>> getProductionLinesStatus() {
        return ResponseEntity.ok(peerService.getAll());
    }

    @GetMapping("/{lineID}")
    public ResponseEntity<Entry<PeerInfo,String>> getProductionline(@PathVariable int lineID){
        return ResponseEntity.ok(peerService.getPeerStatus(lineID));
    }

    @GetMapping("/{lineID}/measurements")
    public ResponseEntity<List<AverageMessage>> getProductionLineMeasurements(@PathVariable int lineID){
        return ResponseEntity.ok(measurementService.getLineMeasurements(lineID));
    }
}