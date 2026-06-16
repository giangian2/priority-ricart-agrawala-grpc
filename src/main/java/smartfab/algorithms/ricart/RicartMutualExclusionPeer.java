package smartfab.algorithms.ricart;

import java.util.Date;

import smartfab.Smartfab.CalibrationRequest;
import smartfab.model.events.CalibrationGrantEvent;
import smartfab.model.events.EventDispatcher;
import smartfab.model.events.EventListener;
import smartfab.model.events.ProductionLineEvent;

/**
 * @author Gianluca Bianchi
 *
 * Ricart-Agrawala mutual exclusion implementation.
 * {@link https://en.wikipedia.org/wiki/Ricart%E2%80%93Agrawala_algorithm}
 *
 * This class no longer extends the transport ({@link GrpcPeer}) it
 * OWNS a {@link Peer}, so the algorithm can be tested with an in-memory transport.
 * The mutable state is delegated to {@link RequestContext},
 * {@link GrantTracker} and {@link DeferredGrants}; the reactions are
 * delegated to the {@link PeerState} objects..
 * All callbacks are synchronized because they are invoked concurrently
 * by multiple gRPC server threads.
 */
public class RicartMutualExclusionPeer implements MutualExclusionAlgorithm, RicartContext {

    private final int                                   peerId;
    private final Peer                                  transport;
    private final EventDispatcher<ProductionLineEvent>  dispatcher;

    private final RequestContext                        request;
    private final GrantTracker                          grants;
    private final DeferredGrants                        deferred;

    private PeerState                                   state;

    public RicartMutualExclusionPeer(Peer transport, int peerId) {
        this.transport  = transport;
        this.peerId     = peerId;
        this.dispatcher = new EventDispatcher<>();
        this.request    = new RequestContext();
        this.grants     = new GrantTracker();
        this.deferred   = new DeferredGrants();
        this.state      = new IdleState();
    }

    @Override
    public void subscribe(EventListener<ProductionLineEvent> listener) {
        this.dispatcher.subscribe(listener);
    }

    @Override
    public synchronized void requestCalibration(double criticality) {
        System.out.println("LINE " + this.peerId + ": Request Calibration");

        this.request.open(criticality, new Date().getTime());
        this.grants.reset();

        if (this.transport.getAllPeers().isEmpty()) {
            this.state = new CalibratingState();
            notifyCalibrationAcquired(this.peerId);
            return;
        }

        this.transport.sendRequestToAll(CalibrationRequest.newBuilder()
                .setLineId(this.peerId)
                .setPriority(criticality)
                .setTimestamp(new Date().getTime())
                .build());

        this.state = new WaitingState();
        System.out.println("LINE " + this.peerId + ": WAITING");
    }

    @Override
    public synchronized void releaseCalibration() {
        this.state = new IdleState();
        this.grants.reset();
        this.deferred.releaseAll(targetId -> this.transport.sendGrant(buildGrant(), targetId));
    }

    @Override
    public synchronized void onRequestReceived(int senderId, double senderCriticality, String senderAddress, int senderPort) {
        this.state.onRequest(this, senderId, senderCriticality);
    }

    @Override
    public synchronized void onGrantReceived(int senderId) {
        this.state.onGrant(this, senderId);
    }

    @Override
    public synchronized void onJoinPeerReceived(int senderId, String senderAddress, int senderPort) {
        System.out.println("NEW PEER JOINED THE NETWORK: " + senderId + "[" + senderAddress + ":" + senderPort + "]");
        this.transport.addPeer(new PeerInfo(senderId, senderAddress, senderPort));

        /* If we are competing, make the newcomer aware of our pending request so
         * it can grant us and we still reach the quorum.
         */
        if (this.state instanceof WaitingState) {
            this.transport.sendRequest(CalibrationRequest.newBuilder()
                    .setLineId(this.peerId)
                    .setPriority(this.request.criticality())
                    .build(), senderId);
        }
    }

    @Override
    public int peerId() {
        return this.peerId;
    }

    @Override
    public double currentCriticality() {
        return this.request.criticality();
    }

    @Override
    public synchronized int otherPeerCount() {
        return this.transport.getAllPeers().size();
    }

    @Override
    public synchronized void grantTo(int targetPeerId) {
        this.transport.sendGrant(buildGrant(), targetPeerId);
    }

    @Override
    public synchronized void deferGrant(int targetPeerId) {
        this.deferred.defer(targetPeerId);
    }

    @Override
    public synchronized void recordGrant(int fromPeerId) {
        this.grants.record(fromPeerId);
    }

    @Override
    public synchronized boolean hasFullQuorum() {
        return this.grants.hasQuorum(otherPeerCount());
    }

    @Override
    public synchronized void enterCriticalSection(int triggeringPeerId) {
        this.state = new CalibratingState();
        notifyCalibrationAcquired(triggeringPeerId);
    }

    private void notifyCalibrationAcquired(int triggeringPeerId) {
        final long now = new Date().getTime();
        new Thread(() -> this.dispatcher.notify(new CalibrationGrantEvent(triggeringPeerId, now))).start();
    }

    private CalibrationRequest buildGrant() {
        return CalibrationRequest.newBuilder()
                .setLineId(this.peerId)
                .setPriority(0)
                .build();
    }
}
