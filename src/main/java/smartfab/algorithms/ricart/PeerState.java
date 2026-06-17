package smartfab.algorithms.ricart;

/**
 * @author Gianluca Bianchi
 * 
 *      State of a peer in the Ricart-Agrawala state machine. Each state knows how to
 *      react to an incoming request and to an incoming grant
 */
interface PeerState {

    /**
     * 
     * @param ctx
     * @param senderId
     * @param senderCriticality
     * @param round
     */
    void onRequest(RicartContext ctx, int senderId, double senderCriticality, int round);

    /**
     * 
     * @param ctx
     * @param senderId
     * @param round
     */
    void onGrant(RicartContext ctx, int senderId, int round);

    /**
     * 
     * @return
     */
    String name();
}