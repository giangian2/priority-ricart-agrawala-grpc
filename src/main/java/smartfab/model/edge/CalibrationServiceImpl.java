package smartfab.model.edge;

import smartfab.CalibrationServiceGrpc.CalibrationServiceImplBase;


public class CalibrationServiceImpl extends CalibrationServiceImplBase{
    
    @Override
    public void requestCalibration(smartfab.Smartfab.CalibrationRequest request,
        io.grpc.stub.StreamObserver<smartfab.Smartfab.Empty> responseObserver) {
  
    }

    @Override
    public void grantCalibrationAccess(smartfab.Smartfab.CalibrationRequest request,
        io.grpc.stub.StreamObserver<smartfab.Smartfab.Empty> responseObserver) {
        
    }

    @Override
    public void joinP2P(smartfab.Smartfab.P2PJoinRequest request,
        io.grpc.stub.StreamObserver<smartfab.Smartfab.P2PJoinResponse> responseObserver) {

    }
}
