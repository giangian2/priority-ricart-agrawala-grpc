package smartfab.model.edge;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import smartfab.algorithms.ricart.PeerInfo;

import java.util.List;

/**
 * @author Gianluca Bianchi
 * 
 *      PeerRestClient used by {@link smartfab.model.edge.ProductionLine}
 *      in order to start the registration process through the http admin server.
 */
public class PeerRestClient {

    private final RestTemplate restTemplate;
    private final String baseUrl;

    public PeerRestClient(String baseUrl) {
        this.restTemplate = new RestTemplate();
        this.baseUrl = baseUrl;
    }

    public List<PeerInfo> registerPeer(PeerInfo newPeer) {
        String url = baseUrl + "/api/prodlines";
        HttpEntity<PeerInfo> requestEntity = new HttpEntity<>(newPeer);

        ResponseEntity<List<PeerInfo>> response = restTemplate.exchange(
                url,
                HttpMethod.POST,
                requestEntity,
                new ParameterizedTypeReference<List<PeerInfo>>() {}
        );

        return response.getBody();
    }

    public void removePeer(PeerInfo peer){
        String url = baseUrl + "/api/prodlines/" + peer.getID();
        restTemplate.exchange(url, HttpMethod.DELETE, HttpEntity.EMPTY, Void.class);
    }
}