package smartfab.algorithms.ricart;

public class PeerInfo {
    private final int       ID;
    private final String    address;
    private final int       port;

    public PeerInfo(int ID, String address, int port){
        this.ID         = ID;
        this.port       = port;
        this.address    = address;
    }

    public int getID(){
        return this.ID;
    }

    public String getAddress(){
        return this.address;
    }

    public int getPort(){
        return this.port;
    }
}
