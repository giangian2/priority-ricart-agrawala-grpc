package smartfab.model.events;

public class CalibrationTerminatedEvent extends ProductionLineEvent{

    public CalibrationTerminatedEvent(int lineId, long timestamp) {
        super(lineId, timestamp);
    }
    
}
