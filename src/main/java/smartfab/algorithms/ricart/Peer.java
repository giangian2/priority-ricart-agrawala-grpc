package smartfab.algorithms.ricart;

import java.util.List;

import smartfab.Smartfab.CalibrationRequest;

/**
 * @author Gianluca Bianchi
 * 
 * This interface will abstract the "Peer" concept into the decentralized system.
 * It will include all the APIs for sending messages to other peers abstracting 
 * the networking layer.
 * In our context, the decentralized system is made about all the production lines
 * that have to coordinate with each other in order to orchestrate the access
 * to critical zones.
 */
public interface Peer {

    /**
     * 
     * @param peer
     */
    void addPeer(PeerInfo peer);

    /**
     * 
     * @param peerId
     */
    void removePeer(int peerId);

    /**
     * 
     * @return
     */
    List<PeerInfo> getAllPeers();

    /**
     * 
     * @param request
     */
    void sendRequestToAll(CalibrationRequest request);

    /**
     * 
     * @param targetLineId
     */
    void sendGrant(int targetLineId);

    /**
     * 
     * @param myId
     */
    void sendReleaseToAll(int myId);
}