package smartfab.algorithms.ricart;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Date;
import java.util.Deque;
import java.util.List;

import smartfab.Smartfab.CalibrationRequest;

/**
 * @author Gianluca Bianchi
 * 
 * The role of this class is to implement Ricart Argwalla Algorithm {@link https://en.wikipedia.org/wiki/Ricart%E2%80%93Agrawala_algorithm}
 * It will use the {@link smartfab.algorithms.ricart.Peer} class to send messages to other peers.
 * All the methods must be synchronized because the instance will receive multiple calls by multiple threads (each for one "peer stub")
 * 
 */
public class RicartMutualExclusionPeer extends GrpcPeer implements MutualExclusionAlgorithm {

    public static enum PeerStatus{
        /**
         * The peer has alreay announced itself through a HELLO messge but others peer doesn't responded with ACK yet
         */
        NOT_CONNECTED,
        /**
         * Does it's work and don't needs to access the critical resource (the calibration state)
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
    private final Deque<PeerInfo>   defferedQueue;
    private final List<PeerInfo>    ackedRequests;

    public RicartMutualExclusionPeer(Peer peer){
        this.status         = RicartMutualExclusionPeer.PeerStatus.IDLE;
        this.defferedQueue  = new ArrayDeque<>();
        this.ackedRequests  = new ArrayList<>();
    }

    public PeerStatus getStatus(){
        return this.status;
    }

    @Override
    public synchronized void requestCalibration(int lineId, double criticality) {
        this.sendRequestToAll(CalibrationRequest.newBuilder()
                .setLineId(lineId)
                .setPriority((int)criticality)
                .setTimestamp(new Date().getTime())
                .build());

        //Stop everything: we need to send the stop notification to the production line in order to stop the threads that reads and parses data

        this.status = PeerStatus.WAITING;
    }

    @Override
    public synchronized void releaseCalibration() {
        //Internalli it will send the GRANT to the other peers into the deffered queue 
    }

    @Override
    public synchronized void onRequestReceived(int senderId, double senderCriticality, String senderAddress, int senderPort) {
        throw new UnsupportedOperationException("Unimplemented method 'onRequestReceived'");
    }

    @Override
    public synchronized void onGrantReceived(int senderId) {
        throw new UnsupportedOperationException("Unimplemented method 'onGrantReceived'");
    }

    @Override
    public synchronized void onReleaseReceived(int senderId) {
        throw new UnsupportedOperationException("Unimplemented method 'onReleaseReceived'");
    }
    
}
