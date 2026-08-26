package smartfab.algorithms.ricart;

/**
 * Not competing: every incoming request is granted right away.
 *
 * Also the state a peer sits in while joining the network, which is why
 * granting unconditionally is the correct behaviour there too.
 */
final class IdleState implements PeerState {

    @Override
    public void onRequest(RicartContext ctx, int senderId, double senderCriticality, int round) {
        ctx.grantTo(senderId, round);
    }

    @Override
    public void onGrant(RicartContext ctx, int senderId, int round) {
        /* not competing: a grant here belongs to an abandoned round */
    }

    @Override
    public String name() {
        return "IDLE";
    }
}
