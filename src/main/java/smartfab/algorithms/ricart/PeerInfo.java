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

    public PeerInfo(int ID){
        this.ID         = ID;
        this.port       = 0;
        this.address    = null;
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

    @Override
    public boolean equals(Object o){
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        var casted = (PeerInfo) o;
        return casted.getID() == this.ID;
    }   
}
