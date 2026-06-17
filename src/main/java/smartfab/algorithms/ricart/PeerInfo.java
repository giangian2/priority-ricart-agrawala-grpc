package smartfab.algorithms.ricart;

/**
 * @author Gianluca Bianchi
 */
public class PeerInfo {
    private final int       ID;
    private final String    address;
    private final int       port;

    public PeerInfo(int ID, String address, int port){
        this.ID         = ID;
        this.port       = port;
        this.address    = address;
    }

    /**
     * 
     * @return
     */
    public int getID(){
        return this.ID;
    }

    /**
     * 
     * @return
     */
    public String getAddress(){
        return this.address;
    }

    /**
     * @return
     */
    public int getPort(){
        return this.port;
    }
}
