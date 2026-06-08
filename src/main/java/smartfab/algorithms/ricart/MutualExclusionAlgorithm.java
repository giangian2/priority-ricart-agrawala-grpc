package smartfab.algorithms.ricart;

/**
 * @author Gianluca Bianchi
 * 
 * Interface for modelling Mutual exclusion algorithm 
 * in order to manage the "grant" of calibration state
 * in the proper way since we are in a distirbuted system.
 */
public interface MutualExclusionAlgorithm {
    /**
     * A Node sends this messagge to all the lines 
     * in order to request the access to claibration mode
     * 
     * @param lineId
     * @param criticality
     */
    void requestCalibration(int lineId, double criticality);

    /**
     * This method is called when a node hash finished using the shared resource,
     * so it ends the CRITICAL SECTIONS and gaves to all the other users the 
     * possibility tho continue concurring for the resource.
     * NOTE: SOME ALGOTIHMS (LIKE RIVCART AND ARGAWALA) DOESN'T HAVE EXPLICITS
     * MESSAGES FOR RELEASE, IT HAPPENS IN AN IMPLICIT WAY THROUGHT A NORMAL
     * GRANT MESSAGE. IN THIS CASE WE CAN IMPLEMNT IT IN A WAY THE RE-SEND A
     * "GRANT" MESSAGE
     */
    void releaseCalibration();

    /**
     * It will be invocated whenever the node receives a resource lock requests.
     * Here we can handle the first step of any algorihtm logic's
     * 
     * @param senderId
     * @param senderCriticality
     * @param senderAddress
     * @param senderPort
     */
    void onRequestReceived(int senderId, double senderCriticality, String senderAddress, int senderPort);

    /**
     * It will be invocated whenever the node receives a GRANT message
     * 
     * @param senderId
     */
    void onGrantReceived(int senderId);

    /**
     * It will be invocated whenever the node receives a RELEASE message
     * 
     * NOTE: THE EFFECTIVE INVOCATION WILL BE HANDLED IN THE IMPLEMENTATION
     * OF THE ALGORITHM, AS WE'VE ALREADY SEEN, SOME ALGORITHMS MAY NOT
     * DEAL WITH ANY KIND OF "RELEASE" MESSAGE. 
     * 
     * @param senderId
     */
    void onReleaseReceived(int senderId);
}