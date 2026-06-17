package smartfab.algorithms.ricart;

import smartfab.model.events.EventListener;
import smartfab.model.events.ProductionLineEvent;

/**
 * @author Gianluca Bianchi
 *
 *      Interface for modelling a mutual exclusion algorithm used to
 *      coordinate the "grant" of the calibration state across the distributed 
 *      production lines.
 *
 *      NOTE: Ricart and Agrawala has no explicit release message (release is an implicit grant).
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
     * @param round
     */
    void onRequestReceived(int senderId, double senderCriticality, int round);

    /**
     * 
     * @param senderId
     * @param round
     */
    void onGrantReceived(int senderId, int round);

    /**
     * Register a listener for the production-line events emitted by the algorithm.
     * @param listener
     */
    void subscribe(EventListener<ProductionLineEvent> listener);
}
