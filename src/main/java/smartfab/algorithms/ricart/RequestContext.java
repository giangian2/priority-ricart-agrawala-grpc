package smartfab.algorithms.ricart;

/**
 * @author Gianluca Bianchi
 * 
 *      Holds the parameters of the calibration request the peer is currently
 *      competing for.
 */
final class RequestContext {

    private volatile double criticality;
    private volatile int    round;

    public void open(double criticality, int round) {
        this.criticality    = criticality;
        this.round          = round;
    }

    public double criticality() {
        return this.criticality;
    }

    public int round() {
        return this.round;
    }

}
