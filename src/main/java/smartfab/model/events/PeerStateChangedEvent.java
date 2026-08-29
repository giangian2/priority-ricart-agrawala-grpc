package smartfab.model.events;

/**
 * @author Gianluca Bianchi
 *
 *      A peer transitioned in the Ricart-Agrawala state machine.
 *
 *      Travels on the same dispatcher as every other production line event
 *      rather than through a listener interface of its own: same pattern, same
 *      ordering guarantee, one place to subscribe.
 */
public class PeerStateChangedEvent extends ProductionLineEvent {

    private final int    peerId;
    private final String stateName;

    public PeerStateChangedEvent(int peerId, String stateName, long timestamp) {
        super(timestamp);
        this.peerId    = peerId;
        this.stateName = stateName;
    }

    public int getPeerId() {
        return this.peerId;
    }

    public String getStateName() {
        return this.stateName;
    }
}
