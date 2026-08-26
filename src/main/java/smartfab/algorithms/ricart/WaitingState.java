package smartfab.algorithms.ricart;

/**
 * Competing for the calibration section: the request has been broadcast and we
 * are collecting grants.
 */
final class WaitingState implements PeerState {

    @Override
    public void onRequest(RicartContext ctx, int senderId, double senderCriticality, int round) {
        final double mine = ctx.myCriticality();

        /*
         * Priority is the pair (criticality, lineId): higher criticality wins,
         * lower lineId breaks the tie. Losing means granting AND competing
         * again from a fresh round, otherwise both peers could end up believing
         * they hold the section.
         */
        final boolean senderWins = senderCriticality > mine
                || (senderCriticality == mine && senderId < ctx.peerId());

        if (senderWins) {
            ctx.grantTo(senderId, round);
            ctx.yieldAndRetry();
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
