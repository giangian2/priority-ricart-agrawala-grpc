package smartfab.model.edge;

import smartfab.CalibrationServiceGrpc.CalibrationServiceImplBase;
import smartfab.algorithms.ricart.MutualExclusionAlgorithm;

/**
 * @author Gianluca bianchi
 *
 *      gRPC entry point: depends only on the {@link MutualExclusionAlgorithm}
 *      abstraction, not on a concrete peer implementation.
 *      FIRE AND FORGET PATTERN: the grpc server will respond immediatly
 *      with an {@link smartfab.Smartfab.Empty} response.
 *      @todo for future improvements we can use a Thread Pool system 
 *      instead of firing anonymous threads.
 *      
 */
public class CalibrationServiceImpl extends CalibrationServiceImplBase {

    private final MutualExclusionAlgorithm peer;

    public CalibrationServiceImpl(MutualExclusionAlgorithm peer) {
        this.peer = peer;
    }

    @Override
    public void exitP2P(smartfab.Smartfab.P2PJoinRequest request,
            io.grpc.stub.StreamObserver<smartfab.Smartfab.Empty> responseObserver) {
      
        System.out.println("[gRPC Server] Received exit from " + request.getLineId());
        new Thread(() -> {
            this.peer.onExitPeerReceived(request.getLineId());
        }).start();
        responseObserver.onNext(smartfab.Smartfab.Empty.getDefaultInstance());
    }

    @Override
    public void requestCalibration(smartfab.Smartfab.CalibrationRequest request,
            io.grpc.stub.StreamObserver<smartfab.Smartfab.Empty> responseObserver) {

        System.out.println("[gRPC Server] Received request calibration from " + request.getLineId());
        new Thread(()->{
            this.peer.onRequestReceived(request.getLineId(), request.getPriority(), (int) request.getTimestamp());
        }).start();
        responseObserver.onNext(smartfab.Smartfab.Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void grantCalibrationAccess(smartfab.Smartfab.CalibrationRequest request,
            io.grpc.stub.StreamObserver<smartfab.Smartfab.Empty> responseObserver) {

        System.out.println("[gRPC Server] Received grant from " + request.getLineId());
        new Thread(()->{
            this.peer.onGrantReceived(request.getLineId(), (int) request.getTimestamp());
        }).start();
        responseObserver.onNext(smartfab.Smartfab.Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void joinP2P(smartfab.Smartfab.P2PJoinRequest request,
            io.grpc.stub.StreamObserver<smartfab.Smartfab.Empty> responseObserver) {

        System.out.println("[gRPC Server] Received join request from " + request.getLineId());
        new Thread(()->{
            this.peer.onJoinPeerReceived(request.getLineId(),request.getSnederAddress(), request.getSenderPort());
        }).start();
        responseObserver.onNext(smartfab.Smartfab.Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }
}
