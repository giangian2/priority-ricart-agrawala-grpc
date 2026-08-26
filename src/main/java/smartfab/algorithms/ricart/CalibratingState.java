package smartfab.algorithms.ricart;

/**
 * Holding the calibration section: every incoming request is deferred and will
 * be granted as a batch on release.
 */
final class CalibratingState implements PeerState {

    @Override
    public void onRequest(RicartContext ctx, int senderId, double senderCriticality, int round) {
        ctx.deferGrant(senderId, round);
    }

    @Override
    public void onGrant(RicartContext ctx, int senderId, int round) {
        /* already in the section: nothing to collect */
    }

    @Override
    public String name() {
        return "CALIBRATING";
    }
}
