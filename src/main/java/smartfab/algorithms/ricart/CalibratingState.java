package smartfab.algorithms.ricart;

/**
 * The peer holds the critical section (it is calibrating): every incoming
 * request is deferred until release, and grants are irrelevant.
 */
final class CalibratingState implements PeerState {

    @Override
    public void onRequest(RicartContext ctx, int senderId, double senderCriticality) {
        ctx.deferGrant(senderId);
    }

    @Override
    public void onGrant(RicartContext ctx, int senderId) {
    }

    @Override
    public String name() {
        return "CALIBRATING";
    }
}
