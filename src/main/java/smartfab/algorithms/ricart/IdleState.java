package smartfab.algorithms.ricart;

/**
 * The peer does not need the critical section: it grants every incoming
 * request immediately and ignores grants.
 */
final class IdleState implements PeerState {

    @Override
    public void onRequest(RicartContext ctx, int senderId, double senderCriticality) {
        ctx.grantTo(senderId);
    }

    @Override
    public void onGrant(RicartContext ctx, int senderId) {
    }

    @Override
    public String name() {
        return "IDLE";
    }
}