package smartfab.algorithms.ricart;

import smartfab.model.events.EventListener;
import smartfab.model.events.ProductionLineEvent;

/**
 * @author Gianluca Bianchi
 *
 * Interface for modelling a mutual exclusion algorithm used to
 * coordinate the "grant" of the calibration state across the distributed 
 * production lines.
 *
 * NOTE: Ricart and Agrawala has no explicit release message (release is an 
 * implicit grant). Algorithms that DO use explicit release messages can declare it
 * through a dedicated sub-interface, so no implementation is forced to throw
 * UnsupportedOperationException.
 */
public interface MutualExclusionAlgorithm {

    /**
     * 
     * @param senderId
     * @param senderAddress
     * @param senderPort
     */
    void onJoinPeerReceived(int senderId, String senderAddress, int senderPort);

    /**
     * 
     * @param criticality
     */
    void requestCalibration(double criticality);

    /**
     * 
     */
    void releaseCalibration();

    /**
     * 
     * @param senderId
     * @param senderCriticality
     * @param senderAddress
     * @param senderPort
     */
    void onRequestReceived(int senderId, double senderCriticality, String senderAddress, int senderPort);

    /**
     * 
     * @param senderId
     */
    void onGrantReceived(int senderId);

    /**
     * Register a listener for the production-line events emitted by the algorithm.
     * @param listener
     */
    void subscribe(EventListener<ProductionLineEvent> listener);
}
