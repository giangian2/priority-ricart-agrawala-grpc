package smartfab.http.respository;

import org.springframework.stereotype.Repository;

import smartfab.algorithms.ricart.PeerInfo;

@Repository
public class PeerRepository extends GlobalLockRepository<PeerInfo, String>{}