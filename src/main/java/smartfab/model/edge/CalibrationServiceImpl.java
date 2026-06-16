package smartfab.model.edge;

import smartfab.CalibrationServiceGrpc.CalibrationServiceImplBase;
import smartfab.algorithms.ricart.MutualExclusionAlgorithm;

/**
 * @author Gianluca bianchi
 *
 * gRPC entry point: depends only on the {@link MutualExclusionAlgorithm}
 * abstraction, not on a concrete peer implementation.
 */
public class CalibrationServiceImpl extends CalibrationServiceImplBase {

    private final MutualExclusionAlgorithm peer;

    public CalibrationServiceImpl(MutualExclusionAlgorithm peer) {
        this.peer = peer;
    }

    @Override
    public void requestCalibration(smartfab.Smartfab.CalibrationRequest request,
            io.grpc.stub.StreamObserver<smartfab.Smartfab.Empty> responseObserver) {

        System.out.println("[gRPC Server] Received request calibration from " + request.getLineId());
        this.peer.onRequestReceived(request.getLineId(), request.getPriority(),
                request.getSnederAddress(), request.getSenderPort());
        responseObserver.onNext(smartfab.Smartfab.Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void grantCalibrationAccess(smartfab.Smartfab.CalibrationRequest request,
            io.grpc.stub.StreamObserver<smartfab.Smartfab.Empty> responseObserver) {

        System.out.println("[gRPC Server] Received grant from " + request.getLineId());
        this.peer.onGrantReceived(request.getLineId());
        responseObserver.onNext(smartfab.Smartfab.Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void joinP2P(smartfab.Smartfab.P2PJoinRequest request,
            io.grpc.stub.StreamObserver<smartfab.Smartfab.Empty> responseObserver) {

        System.out.println("[gRPC Server] Received join request from " + request.getLineId());
        this.peer.onJoinPeerReceived(request.getLineId(),
                request.getSnederAddress(), request.getSenderPort());
        responseObserver.onNext(smartfab.Smartfab.Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }
}
