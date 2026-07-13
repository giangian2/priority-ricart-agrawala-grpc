package smartfab.model.events;

public class CriticalStatusEvent extends ProductionLineEvent{

    private final double criticality;

    public CriticalStatusEvent(long timestamp, double criticality) {
        super(timestamp);
        this.criticality = criticality;
    }

    public double getCriticality(){
        return this.criticality;
    }
}
