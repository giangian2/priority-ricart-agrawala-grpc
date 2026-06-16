package smartfab.algorithms.ricart;

/**
 * Holds the parameters of the calibration request the peer is currently
 * competing for. Replaces the broken {@code criticality} (hard-coded to 0)
 * and the duplicated {@code latestCriticality} field of the old design:
 * now there is a single source of truth for "my pending request".
 */
final class RequestContext {

    private volatile double criticality;
    private volatile long   timestamp;

    void open(double criticality, long timestamp) {
        this.criticality    = criticality;
        this.timestamp      = timestamp;
    }

    double criticality() {
        return this.criticality;
    }

    long timestamp() {
        return this.timestamp;
    }
}
