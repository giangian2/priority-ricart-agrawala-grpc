package smartfab.algorithms.ricart;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Date;
import java.util.Deque;
import java.util.List;

import smartfab.Smartfab.CalibrationRequest;
import smartfab.model.events.CalibrationGrantEvent;
import smartfab.model.events.EventDispatcher;
import smartfab.model.events.EventListener;
import smartfab.model.events.ProductionLineEvent;

/**
 * @author Gianluca Bianchi
 * 
 *         The role of this class is to implement Ricart Argwalla Algorithm
 *         {@link https://en.wikipedia.org/wiki/Ricart%E2%80%93Agrawala_algorithm}
 *         It will use the {@link smartfab.algorithms.ricart.Peer} class to send
 *         messages to other peers.
 *         All the methods must be synchronized because the instance will
 *         receive multiple calls by multiple threads (each for one "peer stub")
 * 
 */
public class RicartMutualExclusionPeer extends GrpcPeer implements MutualExclusionAlgorithm {

    public static enum PeerStatus {
        /**
         * The peer has alreay announced itself through a HELLO messge but others peer
         * doesn't responded with ACK yet
         */
        NOT_CONNECTED,
        /**
         * Does it's work and don't needs to access the critical resource (the
         * calibration state)
         */
        IDLE,
        /**
         * Waits to access te criticla zone for calibration process
         */
        WAITING,
        /**
         * Calibration
         */
        CALIBRATING
    }

    private PeerStatus              status;
    private final int               peerId;
    private final int               criticality;
    private final Deque<Integer>    defferedQueue;
    private final List<Integer>     ackedRequests;

    //DIspatchers
    private final EventDispatcher<ProductionLineEvent>  dispatcher;

    public RicartMutualExclusionPeer(Peer peer, int peerId) {
        this.status         = RicartMutualExclusionPeer.PeerStatus.IDLE;
        this.peerId         = peerId;
        this.criticality    = 0;
        this.defferedQueue  = new ArrayDeque<>();
        this.ackedRequests  = new ArrayList<>();
        this.dispatcher     = new EventDispatcher<>();
    }

    public void subscribe(EventListener<ProductionLineEvent> listener) {
        dispatcher.subscribe(listener);
    }

    public PeerStatus getStatus() {
        return this.status;
    }

    @Override
    public synchronized void requestCalibration(int lineId, double criticality) {

        System.out.println("LINE "+this.peerId+": Request Calibration");

        //If the distributed production line system has only one peer (the current)
        if(this.getAllPeers().size() <= 0){

            this.status = PeerStatus.CALIBRATING;
            this.dispatcher.notify(new CalibrationGrantEvent(lineId, new Date().getTime()));
            return;
        }

        this.sendRequestToAll(CalibrationRequest.newBuilder()
                .setLineId(lineId)
                .setPriority((int) criticality)
                .setTimestamp(new Date().getTime())
                .build());

        // Change internal STATUS
        this.status = PeerStatus.WAITING;

        System.out.println("LINE "+this.peerId+": WAITING");
    }

    @Override
    public synchronized void releaseCalibration() {
        this.status = RicartMutualExclusionPeer.PeerStatus.IDLE;
        this.sendReleaseToAll(this.peerId);
    }

    @Override
    public synchronized void onRequestReceived(int senderId, double senderCriticality, String senderAddress,
            int senderPort) {

        // If the peer is in IDEL it will respon OK to all the incoming calibration
        // requests
        if (this.status.equals(RicartMutualExclusionPeer.PeerStatus.IDLE)) {
            this.sendGrant(senderId);
        }

        if (this.status.equals(RicartMutualExclusionPeer.PeerStatus.CALIBRATING)) {
            /**
             * @todo
             */
        }

        // Insead of using Lamport based on Logical Clock
        if (this.status.equals(RicartMutualExclusionPeer.PeerStatus.WAITING)) {
            if (senderCriticality > this.criticality) {
                this.sendGrant(senderId);
            } else if (senderCriticality < this.criticality) {
                this.defferedQueue.add(senderId);
            } else {
                if (senderId < this.peerId) {
                    this.sendGrant(senderId);
                } else {
                    this.defferedQueue.add(senderId);
                }
            }
        }
    }

    @Override
    public synchronized void onGrantReceived(int senderId) {

        if (this.status.equals(RicartMutualExclusionPeer.PeerStatus.WAITING)) {
            this.defferedQueue.add(senderId);

            if (this.checkGrant()) {
                this.status = RicartMutualExclusionPeer.PeerStatus.CALIBRATING;
                this.dispatcher.notify(new CalibrationGrantEvent(senderId, new Date().getTime()));
            }
        }
    }

    @Override
    public synchronized void onReleaseReceived(int senderId) {
        throw new UnsupportedOperationException("Unimplemented method 'onReleaseReceived'");
    }

    private synchronized boolean checkGrant() {
        return (this.getAllPeers().size() - 1) == this.ackedRequests.stream().distinct().count();
    }

    @Override
    public synchronized void joinPeer(int senderId, String senderAddress, int senderPort) {
        System.out.println("NEW PEER JOINED THE NETWORK: "+senderId+ "["+senderAddress+":"+senderPort+"]");
        this.addPeer(new PeerInfo(senderId, senderAddress, senderPort));

        if(this.getStatus().equals(RicartMutualExclusionPeer.PeerStatus.WAITING)){
            this.sendGrant(senderId);
        }
    }
}
