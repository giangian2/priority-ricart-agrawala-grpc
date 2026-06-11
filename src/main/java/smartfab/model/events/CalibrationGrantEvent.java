package smartfab.model.events;

public class CalibrationGrantEvent extends ProductionLineEvent{

    public CalibrationGrantEvent(int lineId, long timestamp) {
        super(lineId, timestamp);
    }
    
}
