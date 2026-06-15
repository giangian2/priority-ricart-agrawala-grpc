package smartfab.model.events;

public class ProductionLineEvent {
    
    private final int   lineId;
    private final long  timestamp;

    public ProductionLineEvent(int lineId, long timestamp){
        this.lineId     = lineId;
        this.timestamp  = timestamp;
    }

    public int getLineId(){
        return this.lineId;
    }

    public long getTimestamp(){
        return this.timestamp;
    }
}
