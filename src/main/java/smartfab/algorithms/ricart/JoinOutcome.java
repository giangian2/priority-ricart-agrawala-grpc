package smartfab.algorithms.ricart;

import java.util.List;

/**
 * @author Gianluca Bianchi
 *
 *      Result of the network join handshake.
 *
 * @param confirmed   peers that acknowledged our join request and are therefore
 *                    part of the topology
 * @param rejected    peers that failed or were unreachable: they are NOT part
 *                    of the topology and never entered the quorum
 * @param allAnswered false when the deadline expired before every candidate
 *                    replied
 */
public record JoinOutcome(List<PeerInfo> confirmed, List<PeerInfo> rejected, boolean allAnswered) {}
