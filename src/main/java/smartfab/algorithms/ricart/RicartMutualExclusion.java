package smartfab.algorithms.ricart;

/**
 * @author Gianluca Bianchi
 * 
 * The role of this class is to implement Ricart Argwalla Algorithm {@link https://en.wikipedia.org/wiki/Ricart%E2%80%93Agrawala_algorithm}
 * It will use the {@link smartfab.algorithms.ricart.Peer} class to send messages to other peers.
 * 
 */
public class RicartMutualExclusion implements MutualExclusionAlgorithm {

    @Override
    public void requestCalibration(int lineId, double criticality) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'requestCalibration'");
    }

    @Override
    public void releaseCalibration() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'releaseCalibration'");
    }

    @Override
    public void onRequestReceived(int senderId, double senderCriticality, String senderAddress, int senderPort) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'onRequestReceived'");
    }

    @Override
    public void onGrantReceived(int senderId) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'onGrantReceived'");
    }

    @Override
    public void onReleaseReceived(int senderId) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'onReleaseReceived'");
    }
    
}
