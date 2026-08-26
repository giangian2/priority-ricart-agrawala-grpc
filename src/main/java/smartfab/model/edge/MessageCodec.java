package smartfab.model.edge;

import smartfab.Smartfab.CalibrationRequest;
import smartfab.Smartfab.P2PJoinRequest;
import smartfab.algorithms.ricart.PeerInfo;
import smartfab.algorithms.ricart.PeerMessage;

/**
 * @author Gianluca Bianchi
 *
 *      The ONLY place where the domain messages of the algorithm meet the
 *      protobuf wire format.
 *
 *      Because of this, moving from four unary RPCs to a single persistent
 *      stream (see GRPC_STREAMING_DESIGN.md) changes this class and the
 *      transport adapter, and nothing inside smartfab.algorithms.ricart.
 *
 *      NOTE: on Java 17 a switch over a sealed interface is not exhaustive yet
 *      (pattern matching for switch is standard from 21), hence the instanceof
 *      chain closed by a throw. On 21 this becomes a compiler checked switch.
 */
public final class MessageCodec {

    private MessageCodec() { }

    /**
     * Both a request and a grant travel as a CalibrationRequest today: the
     * legacy proto has no dedicated reply message. The distinction is carried
     * by the RPC being invoked, not by the payload, which is exactly why the
     * algorithm must not see these types.
     */
    public static CalibrationRequest toProto(PeerMessage.Request request) {
        return CalibrationRequest.newBuilder()
                .setLineId(request.senderId())
                .setPriority(request.criticality())
                .setTimestamp(request.round())
                .build();
    }

    public static CalibrationRequest toProto(PeerMessage.Grant grant) {
        return CalibrationRequest.newBuilder()
                .setLineId(grant.senderId())
                .setTimestamp(grant.round())
                .build();
    }

    public static P2PJoinRequest toProto(PeerMessage.Leave leave) {
        return P2PJoinRequest.newBuilder()
                .setLineId(leave.senderId())
                .build();
    }

    public static P2PJoinRequest joinRequest(PeerInfo me) {
        return P2PJoinRequest.newBuilder()
                .setLineId(me.getID())
                .setSnederAddress(me.getAddress())
                .setSenderPort(me.getPort())
                .build();
    }

    public static PeerMessage.Request toRequest(CalibrationRequest proto) {
        return new PeerMessage.Request(proto.getLineId(), proto.getPriority(), (int) proto.getTimestamp());
    }

    public static PeerMessage.Grant toGrant(CalibrationRequest proto) {
        return new PeerMessage.Grant(proto.getLineId(), (int) proto.getTimestamp());
    }
}
