package smartfab.algorithms.ricart;

final class CalibratingState implements PeerState {

    @Override
    public void onRequest(RicartContext ctx, int senderId, double senderCriticality,int round) {
        ctx.deferGrant(senderId, round);
    }

    @Override
    public void onGrant(RicartContext ctx, int senderId, int roound) {
    }

    @Override
    public String name() {
        return "CALIBRATING";
    }
}
