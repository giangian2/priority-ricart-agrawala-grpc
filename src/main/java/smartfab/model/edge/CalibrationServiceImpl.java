package smartfab.model.edge;

import io.grpc.stub.StreamObserver;
import smartfab.CalibrationServiceGrpc.CalibrationServiceImplBase;
import smartfab.Smartfab.CalibrationRequest;
import smartfab.Smartfab.Empty;
import smartfab.Smartfab.P2PJoinRequest;
import smartfab.algorithms.ricart.PeerEventHandler;

/**
 * @author Gianluca Bianchi
 *
 *      gRPC entry point.
 *
 *      Depends on {@link PeerEventHandler} only, never on the full algorithm:
 *      the network layer must be able to DELIVER events, not to issue
 *      application commands such as requestCalibration from a network thread.
 */
public class CalibrationServiceImpl extends CalibrationServiceImplBase {

    private final PeerEventHandler handler;

    public CalibrationServiceImpl(PeerEventHandler handler) {
        this.handler = handler;
    }

    /**
     * INVARIANT: the state is updated BEFORE the response is closed.
     *
     * The joining peer treats the completion of this RPC as the proof that we
     * already registered it, and only then does it register us back. Answering
     * first and working afterwards would silently break that guarantee.
     */
    @Override
    public void joinP2P(P2PJoinRequest request, StreamObserver<Empty> responseObserver) {
        System.out.println("[gRPC Server] Received join request from " + request.getLineId());

        this.handler.onJoinPeerReceived(
                request.getLineId(),
                request.getSnederAddress(),
                request.getSenderPort());

        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void exitP2P(P2PJoinRequest request, StreamObserver<Empty> responseObserver) {
        System.out.println("[gRPC Server] Received exit from " + request.getLineId());

        this.handler.onExitPeerReceived(request.getLineId());

        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void requestCalibration(CalibrationRequest request, StreamObserver<Empty> responseObserver) {
        System.out.println("[gRPC Server] Received request calibration from " + request.getLineId());

        var message = MessageCodec.toRequest(request);
        this.handler.onRequestReceived(message.senderId(), message.criticality(), message.round());

        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void grantCalibrationAccess(CalibrationRequest request, StreamObserver<Empty> responseObserver) {
        System.out.println("[gRPC Server] Received grant from " + request.getLineId());

        var message = MessageCodec.toGrant(request);
        this.handler.onGrantReceived(message.senderId(), message.round());

        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }
}
