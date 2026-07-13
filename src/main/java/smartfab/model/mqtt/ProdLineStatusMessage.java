package smartfab.model.mqtt;

public class ProdLineStatusMessage {
    
    private final int       lineId;
    private final String    status;

    public ProdLineStatusMessage(int lineId, String status){
        this.lineId     = lineId;
        this.status     = status;
    }

    public int getLineId(){
        return this.lineId;
    }

    public String getStatus(){
        return this.status;
    }
}
