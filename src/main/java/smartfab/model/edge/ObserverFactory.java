package smartfab.model.edge;

import io.grpc.stub.StreamObserver;
import smartfab.Smartfab.Empty;
import smartfab.Smartfab.P2PJoinResponse;

public class ObserverFactory {
    
    public StreamObserver<Empty> emptyStreamObserver(){
        return new StreamObserver<Empty>(){

            @Override
            public void onNext(Empty value) {}

            @Override
            public void onError(Throwable t) {
                t.printStackTrace();
            }

            @Override
            public void onCompleted() {}

        };
    }

    public StreamObserver<P2PJoinResponse> joinResponseStreamObserver(){
        return new StreamObserver<P2PJoinResponse>(){

            @Override
            public void onNext(P2PJoinResponse value) {
                // TODO Auto-generated method stub
                throw new UnsupportedOperationException("Unimplemented method 'onNext'");
            }

            @Override
            public void onError(Throwable t) {
                // TODO Auto-generated method stub
                throw new UnsupportedOperationException("Unimplemented method 'onError'");
            }

            @Override
            public void onCompleted() {
                // TODO Auto-generated method stub
                throw new UnsupportedOperationException("Unimplemented method 'onCompleted'");
            }

        };
    }
        
}
