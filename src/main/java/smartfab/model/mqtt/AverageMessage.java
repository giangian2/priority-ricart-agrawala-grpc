package smartfab.model.mqtt;

public class AverageMessage {
    
    private final int       lineId;
    private final double    avg;
    private final long      timestamp;
    
    public AverageMessage(int lineId, double avg, long timestamp){
        this.lineId     = lineId;
        this.avg        = avg;
        this.timestamp  = timestamp;
    }

    public int getLineId(){
        return this.lineId;
    }

    public double getAvg(){
        return this.avg;
    }

    public long getTimestamp(){
        return this.timestamp;
    }
}
