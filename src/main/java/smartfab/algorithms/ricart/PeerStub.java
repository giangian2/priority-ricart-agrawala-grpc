package smartfab.model.edge;

import io.grpc.ManagedChannel;
import smartfab.CalibrationServiceGrpc.CalibrationServiceStub;

public class PeerStub {
    private final ManagedChannel            channel;
    private final CalibrationServiceStub    stub;

    public PeerStub(ManagedChannel channel, CalibrationServiceStub stub) {
        this.channel    = channel;
        this.stub       = stub;
    }

    public CalibrationServiceStub getStub() {
        return stub;
    }

    public void shutdown() {
        channel.shutdown();
    }
}
