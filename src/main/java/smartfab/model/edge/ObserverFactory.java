package smartfab.model.edge;

import io.grpc.stub.StreamObserver;
import smartfab.Smartfab.Empty;

/**
 * @author Gianluca Bianchi
 */
public class ObserverFactory {
    
    /**
     * Return a empty tream observer, it will replicate the behavior of
     * ASYNC and TRANSIENT communications: the "peer" that sends any message
     * will not listen the observer in another thread. The "receiver" peer
     * will send another message to the previous peer.
     * Note that this assumption is particularly relevant: we assume that 
     * gRPC communications in the context of mutual exclusion algorithm are 
     * ONE-DIRECTIONAL. A Response is not handled through {@link io.grpc.stub.StreamObserver},
     * but with another incominc GRPC Request.
     * @return {@link io.grpc.stub.StreamObserver}
     */
    public StreamObserver<Empty> emptyStreamObserver(){
        return new StreamObserver<Empty>(){

            @Override
            public void onNext(Empty value) {
            }

            @Override
            public void onError(Throwable t) {
                System.out.println("STREAM ERROR:");
                t.printStackTrace();
            }

            @Override
            public void onCompleted() {
                System.out.println("COMPLETED");
            }
        };
    }
}
