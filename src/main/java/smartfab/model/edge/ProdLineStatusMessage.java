package smartfab.model.edge;

public class ProdLineStatusMessage {
    
    private final int       lineId;
    private final String    status;
    private final long      timestamp;

    public ProdLineStatusMessage(int lineId, String status, long timestamp){
        this.lineId     = lineId;
        this.status     = status;
        this.timestamp  = timestamp;
    }

    public int getId(){
        return this.lineId;
    }

    public String getStatus(){
        return this.status;
    }

    public long getTimestamp(){
        return this.timestamp;
    }
}
