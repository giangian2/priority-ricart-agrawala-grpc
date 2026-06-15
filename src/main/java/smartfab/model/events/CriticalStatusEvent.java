package smartfab.model.events;

public class CriticalStatusEvent extends ProductionLineEvent{

    private final int criticality;

    public CriticalStatusEvent(int lineId, long timestamp, int criticality) {
        super(lineId, timestamp);
        this.criticality = criticality;
    }

    public int getCriticality(){
        return this.criticality;
    }
    
}
