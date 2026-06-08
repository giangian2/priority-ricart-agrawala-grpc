package smartfab.algorithms.ricart;

/**
 * @author Gianluca Bianchi
 * 
 * The role of this class is to implement Ricart Argwalla Algorithm {@link https://en.wikipedia.org/wiki/Ricart%E2%80%93Agrawala_algorithm}
 * It will use the {@link smartfab.algorithms.ricart.Peer} class to send messages to other peers.
 * 
 */
public class RicartMutualExclusionPeer implements MutualExclusionAlgorithm {

    private final Peer peer;

    public RicartMutualExclusionPeer(Peer peer){
        this.peer = peer;
    }

    @Override
    public synchronized void requestCalibration(int lineId, double criticality) {
        throw new UnsupportedOperationException("Unimplemented method 'requestCalibration'");
    }

    @Override
    public synchronized void releaseCalibration() {
        throw new UnsupportedOperationException("Unimplemented method 'releaseCalibration'");
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
