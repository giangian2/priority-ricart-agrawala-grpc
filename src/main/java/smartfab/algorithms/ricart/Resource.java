package smartfab.algorithms.ricart;

public class Resource{
    private final String ID;

    public Resource(String ID){
        this.ID=ID;
    }

    public String getID(){
        return this.ID;
    }

    @Override
    public boolean equals(Object o){
        return false;
    }
}
