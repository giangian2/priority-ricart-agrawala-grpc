package smartfab.algorithms.ricart;

/**
 * State of a peer in the Ricart-Agrawala state machine. Each state knows how to
 * react to an incoming request and to an incoming grant, removing the
 * if-(status == ...) chains from the algorithm class. Every state handles both
 * events explicitly, so no case can be silently forgotten.
 */
interface PeerState {

    void onRequest(RicartContext ctx, int senderId, double senderCriticality);

    void onGrant(RicartContext ctx, int senderId);

    String name();
}