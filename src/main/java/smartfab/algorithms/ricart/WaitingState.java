package smartfab.algorithms.ricart;


final class WaitingState implements PeerState {

    @Override
    public void onRequest(RicartContext ctx, int senderId, double senderCriticality, int round) {
        double mine = ctx.currentCriticality();

        if (senderCriticality > mine) {
            ctx.grantTo(senderId,round); 
            ctx.restart();
        } else if (senderCriticality < mine) {
            ctx.deferGrant(senderId, round);
        } else if (senderId < ctx.peerId()) {
            ctx.grantTo(senderId, round); 
            ctx.restart();
        } else {
            ctx.deferGrant(senderId, round);
        }
    }

    @Override
    public void onGrant(RicartContext ctx, int senderId, int round) {
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