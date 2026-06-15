package smartfab.model.edge;

import smartfab.CalibrationServiceGrpc.CalibrationServiceImplBase;
import smartfab.algorithms.ricart.PeerInfo;
import smartfab.algorithms.ricart.RicartMutualExclusionPeer;

/**
 * @author Gianluca bianchi
 */
public class CalibrationServiceImpl extends CalibrationServiceImplBase{

    private final RicartMutualExclusionPeer peer;

    public CalibrationServiceImpl(RicartMutualExclusionPeer peer){
        this.peer = peer;
    }
    
    @Override
    public void requestCalibration(smartfab.Smartfab.CalibrationRequest request,
        io.grpc.stub.StreamObserver<smartfab.Smartfab.Empty> responseObserver) {
            
            this.peer.onRequestReceived(request.getLineId(), request.getPriority(), request.getSnederAddress(), request.getSenderPort());
            responseObserver.onNext(smartfab.Smartfab.Empty.getDefaultInstance());
            responseObserver.onCompleted();
    }

    @Override
    public void grantCalibrationAccess(smartfab.Smartfab.CalibrationRequest request,
        io.grpc.stub.StreamObserver<smartfab.Smartfab.Empty> responseObserver) {
        
            this.peer.onGrantReceived(request.getLineId());
            responseObserver.onNext(smartfab.Smartfab.Empty.getDefaultInstance());
            responseObserver.onCompleted();
    }

    @Override
    public void joinP2P(smartfab.Smartfab.P2PJoinRequest request,
        io.grpc.stub.StreamObserver<smartfab.Smartfab.Empty> responseObserver) {

            this.peer.joinPeer(request.getLineId(), request.getSnederAddress(), request.getSenderPort());
            responseObserver.onNext(smartfab.Smartfab.Empty.getDefaultInstance());
            responseObserver.onCompleted();
    }
}
