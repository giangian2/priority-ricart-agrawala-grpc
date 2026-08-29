package smartfab.algorithms.ricart;

import java.util.List;

import smartfab.model.events.EventListener;
import smartfab.model.events.ProductionLineEvent;

/**
 * @author Gianluca Bianchi
 *
 *      Application facing side of the mutual exclusion algorithm: what the
 *      production line ASKS of it.
 *
 *      Everything that ARRIVES from the network lives in
 *      {@link PeerEventHandler} instead, so that the gRPC layer cannot invoke
 *      an application command from a network thread.
 *
 *      NOTE: Ricart and Agrawala has no explicit release message: releasing is
 *      an implicit grant to every deferred peer.
 */
public interface MutualExclusionAlgorithm {

    /**
     * Joins the peer network, registering ONLY the peers that acknowledge us.
     *
     * Until this returns, requestCalibration is refused: with an empty topology
     * the peer would believe it is alone and enter the section immediately.
     *
     * @param fromRegistry  peers returned by the registration server
     * @param timeoutMillis how long to wait for the acknowledgements
     * @return true if every candidate answered within the deadline
     */
    boolean join(List<PeerInfo> fromRegistry, long timeoutMillis);

    /**
     * @param criticality the priority of this request: higher wins
     */
    void requestCalibration(double criticality);

    void releaseCalibration();

    /**
     * Leaves the network: waits for a calibration in progress, flushes the
     * deferred grants, announces the exit and closes the transport.
     */
    void shutdown();

    void subscribe(EventListener<ProductionLineEvent> listener);
}
