package smartfab.algorithms.ricart;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Gianluca Bianchi
 *
 *      The peers that count towards the quorum.
 *
 *      This is the SINGLE source of truth for membership: holding an open
 *      channel is not the same as being part of the network. A peer enters here
 *      only after it acknowledged our join request (or after it announced
 *      itself to us), never just because a channel was opened towards it.
 *
 *      NOT thread-safe on purpose: it is owned by {@link RicartEngine} and is
 *      always accessed under the engine monitor. Adding a second lock here
 *      would only be redundant.
 */
final class PeerRegistry {

    private final Map<Integer, PeerInfo> members = new HashMap<>();

    void add(PeerInfo peer) {
        this.members.put(peer.getID(), peer);
    }

    void addAll(Collection<PeerInfo> peers) {
        peers.forEach(this::add);
    }

    boolean remove(int peerId) {
        return this.members.remove(peerId) != null;
    }

    boolean contains(int peerId) {
        return this.members.containsKey(peerId);
    }

    /** @return the ids of every peer in the network, self excluded */
    Set<Integer> ids() {
        return Set.copyOf(this.members.keySet());
    }

    List<PeerInfo> all() {
        return new ArrayList<>(this.members.values());
    }

    /** @return how many OTHER peers are in the network: the quorum size */
    int size() {
        return this.members.size();
    }

    void clear() {
        this.members.clear();
    }
}
