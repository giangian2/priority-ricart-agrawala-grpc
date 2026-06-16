package smartfab.model.events;

public class ProductionLineEvent {
    
    private final long  timestamp;

    public ProductionLineEvent(long timestamp){
        this.timestamp  = timestamp;
    }

    public long getTimestamp(){
        return this.timestamp;
    }
}
