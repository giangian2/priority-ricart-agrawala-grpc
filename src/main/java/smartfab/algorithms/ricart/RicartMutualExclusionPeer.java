package smartfab.algorithms.ricart;

import java.util.Date;

import org.eclipse.paho.client.mqttv3.MqttException;

import smartfab.Smartfab.CalibrationRequest;
import smartfab.model.events.CalibrationGrantEvent;
import smartfab.model.events.EventDispatcher;
import smartfab.model.events.EventListener;
import smartfab.model.events.ProductionLineEvent;
import smartfab.model.mqtt.MqttClientManager;

/**
 * @author Gianluca Bianchi
 *
 *         Ricart-Agrawala mutual exclusion implementation.
 *         {@link https://en.wikipedia.org/wiki/Ricart%E2%80%93Agrawala_algorithm}
 *
 *         This class no longer extends the transport ({@link GrpcPeer}) it
 *         OWNS a {@link Peer}, so the algorithm can be tested with an in-memory
 *         transport.
 */
public class RicartMutualExclusionPeer implements MutualExclusionAlgorithm, RicartContext {

    private volatile int    round;
    private PeerState       state;

    private final int               peerId;
    private final Peer              transport;
    private final RequestContext    request;
    private final GrantTracker      grants;
    private final DeferredGrants    deferred;

    private final EventDispatcher<ProductionLineEvent>  dispatcher;

    public RicartMutualExclusionPeer(Peer transport, int peerId) {
        this.transport  = transport;
        this.peerId     = peerId;
        this.dispatcher = new EventDispatcher<>();
        this.request    = new RequestContext();
        this.grants     = new GrantTracker();
        this.deferred   = new DeferredGrants();
        this.state      = new IdleState();
        this.round      = 0;
    }

    private synchronized void setState(PeerState newState){
        this.state = newState;
        try {
            MqttClientManager.getInstance()
                .publishNewState(peerId, newState.name());
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    @Override
    public synchronized void subscribe(EventListener<ProductionLineEvent> listener) {
        this.dispatcher.subscribe(listener);
    }

    @Override
    public synchronized void requestCalibration(double criticality) {
        System.out.println("LINE " + this.peerId + ": Request Calibration");

        if (this.state instanceof CalibratingState) {
            System.out.println("LINE " + this.peerId + ": already CALIBRATING, trigger ignored");
            return;
        }

        /**
         * INCREMENT THE ROUND AT EACH NEW CALIBRATION REQUEST
         */
        this.round++;
        System.out.println("ROUND: "+ this.round);

        this.request.open(criticality, this.round);
        this.grants.reset();

        if (this.transport.getAllPeers().isEmpty()) {
            this.setState(new CalibratingState());
            notifyCalibrationAcquired(this.peerId);
            return;
        }

        this.transport.sendRequestToAll(CalibrationRequest.newBuilder()
                .setLineId(this.peerId)
                .setPriority(criticality)
                .setTimestamp(this.request.round())
                .build());

        this.setState(new WaitingState());
        System.out.println("LINE " + this.peerId + ": WAITING");
    }

    @Override
    public synchronized void releaseCalibration() {
        this.setState(new IdleState());
        this.grants.reset();
        this.deferred.releaseAll((targetId,round) -> this.transport.sendGrant(CalibrationRequest.newBuilder()
                .setLineId(this.peerId)
                .setTimestamp(round)
                .setPriority(0)
                .build(), targetId));

        notifyAll();
    }

    @Override
    public synchronized void onRequestReceived(int senderId, double senderCriticality, int round) {
        this.state.onRequest(this, senderId, senderCriticality, round);
    }

    @Override
    public synchronized void onGrantReceived(int senderId, int round) {
        /**
         * The grant handler is invoked only when the grant mathes the current peer's round.
         * It is foundamental to guarantee mutual exclusion in async systems
         */
        if(this.request.round() == round){
            this.state.onGrant(this, senderId, round);
        }else{
            System.out.println("ROUND DOES NOT MATCH!");
        }
    }

    @Override
    public synchronized void onExitPeerReceived(int senderId) {
        System.out.println("PEER LEFT NETWORK: " + senderId);
        this.transport.removePeer(senderId);
        this.deferred.remove(senderId);
        this.grants.forget(senderId);

        if (this.state instanceof WaitingState && this.hasFullQuorum()) {
            this.enterCriticalSection(this.peerId);
        }
    }

    @Override
    public synchronized void onJoinPeerReceived(int senderId, String senderAddress, int senderPort) {
        System.out.println("NEW PEER JOINED THE NETWORK: " + senderId + "[" + senderAddress + ":" + senderPort + "]");
        this.transport.addPeer(new PeerInfo(senderId, senderAddress, senderPort));

        /*
         * If we are competing, make the newcomer aware of our pending request so
         * it can grant us and we still reach the quorum.
         * WE CAN'T REUSE THE requerstCalibration() METHOD BECAUSE IT CLEARS 
         * THE RECEIVED GRANTS CACHE
         */
        if (this.state instanceof WaitingState) {
            this.transport.sendRequest(CalibrationRequest.newBuilder()
                    .setLineId(this.peerId)
                    .setTimestamp(this.request.round())
                    .setPriority(this.request.criticality())
                    .build(), senderId);
        }
    }

    @Override
    public int peerId() {
        return this.peerId;
    }

    @Override
    public synchronized double currentCriticality() {
        return this.request.criticality();
    }

    @Override
    public int otherPeerCount() {
        return this.transport.getAllPeers().size();
    }

    @Override
    public void grantTo(int targetPeerId, int round) {
        this.transport.sendGrant(CalibrationRequest.newBuilder()
                .setLineId(this.peerId)
                .setTimestamp(round)
                .setPriority(0)
                .build(), targetPeerId);
    }

    @Override
    public void deferGrant(int targetPeerId, int round) {
        this.deferred.defer(targetPeerId, round);
    }

    @Override
    public void recordGrant(int fromPeerId) {
        this.grants.record(fromPeerId);
    }

    @Override
    public boolean hasFullQuorum() {
        return this.grants.hasQuorum(this.otherPeerCount());
    }

    @Override
    public synchronized void enterCriticalSection(int triggeringPeerId) {
        this.setState(new CalibratingState());
        notifyCalibrationAcquired(triggeringPeerId);
    }

    @Override
    public synchronized void restart(){
        System.out.println("RESTART CALIBRATION REQUEST");
        this.requestCalibration(this.currentCriticality());
    }

    /**
     * 
     * @param triggeringPeerId
     */
    private void notifyCalibrationAcquired(int triggeringPeerId) {
        final long now = new Date().getTime();
        // new Thread(() -> this.dispatcher.notify(new
        // CalibrationGrantEvent(triggeringPeerId, now))).start();
        this.dispatcher.notify(new CalibrationGrantEvent(triggeringPeerId, now));
    }

    @Override
    public synchronized void shutdown() {
        while (this.state instanceof CalibratingState) {
            System.out.println("Shutdown: attendo fine calibrazione");
            try {
                wait();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        if (this.state instanceof WaitingState) {
            this.releaseCalibration();
        } else {
            this.grants.reset();
            this.deferred.clear();
        }

        this.transport.sendExitToAll(this.peerId);
        this.transport.shutdown();
        System.out.println("Shutdown completato");
    }
}
