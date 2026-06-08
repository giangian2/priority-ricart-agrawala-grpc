package smartfab.algorithms.ricart;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Queue;

/**
 * @author Gianluca Bianchi
 * 
 * The role of this class is to implement Ricart Argwalla Algorithm {@link https://en.wikipedia.org/wiki/Ricart%E2%80%93Agrawala_algorithm}
 * It will use the GRPC stubs created with other nodes in order to exchange messages throught ProtoBuff.
 * 
 */
public class RicartMutualExclusionService {
    
    private List<Resource>  resources;
    private List<Integer>   grantedNodes;
    private Deque<Integer>  waitingNodes;
    
    public RicartMutualExclusionService(List<Resource> resource){
        this.resources      = resource;
        this.grantedNodes   = new ArrayList<>();
        this.waitingNodes   = new ArrayDeque<>();
    }

    
}
