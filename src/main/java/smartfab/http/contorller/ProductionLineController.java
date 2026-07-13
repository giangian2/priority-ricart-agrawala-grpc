package smartfab.http.contorller;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

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
    private PeerService peerService;
    @Autowired
    private MeasurementService measurementService;

    @PostMapping
    public ResponseEntity<List<PeerInfo>> registerProductionLine(@RequestBody PeerInfo peer) {
        Map<PeerInfo, String> updatedPeers = peerService.getAll();
        peerService.registerPeer(peer);
        return ResponseEntity.ok(new ArrayList<>(updatedPeers.keySet()));
    }

    @GetMapping
    public ResponseEntity<List<PeerStatusResponse>> getProductionLinesStatus() {
        return ResponseEntity.ok(
                peerService.getAll()
                        .entrySet()
                        .stream()
                        .map((entry) -> new PeerStatusResponse(
                                entry.getKey().getID(),
                                entry.getKey().getAddress(),
                                entry.getKey().getPort(),
                                entry.getValue()))
                        .collect(Collectors.toList()));
    }

    @GetMapping("/{lineID}")
    public ResponseEntity<PeerStatusResponse> getProductionline(@PathVariable int lineID) {
        var peerEntry = peerService.getPeer(lineID);
        return ResponseEntity.ok(new PeerStatusResponse(
                peerEntry.getKey().getID(), 
                peerEntry.getKey().getAddress(), 
                peerEntry.getKey().getPort(), 
                peerEntry.getValue()));
    }

    @GetMapping("/{lineID}/measurements")
    public ResponseEntity<List<AverageMessage>> findProductionLineMeasurements(
                @PathVariable int lineID,
                @RequestParam Optional<Long> from,
                @RequestParam Optional<Long> to ) {

        if(from.isEmpty() || to.isEmpty()){
            from    = Optional.of((new Date().getTime()) - 60000);
            to      = Optional.of(new Date().getTime()); 
        }
        return ResponseEntity.ok(measurementService.findLineMeasurements(lineID, from.get(), to.get()));
    }

    @DeleteMapping("/{lineID}")
    public void deleteProductionLine(@PathVariable int lineID){
        this.peerService.removePeer(new PeerInfo(lineID,null,0));
    }
    /**
     * 
     * PeerStatusResponse
     * @param id
     * @param address
     * @param port
     * @param status
     */
    public record PeerStatusResponse(int id, String address, int port, String status) {
    }
}