package smartfab.algorithms.ricart;

/**
 * The peer is competing for the critical section. Incoming requests are ordered
 * against the local request using the total order (criticality, then id):
 * higher criticality wins; on a tie the lower id wins.
 */
final class WaitingState implements PeerState {

    @Override
    public void onRequest(RicartContext ctx, int senderId, double senderCriticality) {
        double mine = ctx.currentCriticality();

        if (senderCriticality > mine) {
            ctx.grantTo(senderId); 
        } else if (senderCriticality < mine) {
            ctx.deferGrant(senderId);
        } else if (senderId < ctx.peerId()) {
            ctx.grantTo(senderId); 
        } else {
            ctx.deferGrant(senderId);
        }
    }

    @Override
    public void onGrant(RicartContext ctx, int senderId) {
        ctx.recordGrant(senderId);
        if (ctx.hasFullQuorum()) {
            ctx.enterCriticalSection(senderId);
        }
    }

    @Override
    public String name() {
        return "WAITING";
    }
}