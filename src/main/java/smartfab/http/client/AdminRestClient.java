package smartfab.http.client;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import smartfab.http.contorller.ProductionLineController.PeerStatusResponse;
import smartfab.model.mqtt.AverageMessage;

import java.util.List;

/**
 * @author Gianluca Bianchi
 *
 *      REST client used by the admin CLI to query the admin server.
 */
public class AdminRestClient {

    private static final String BASE_PATH = "/api/prodlines";

    private final RestTemplate restTemplate;
    private final String baseUrl;

    public AdminRestClient(String baseUrl) {
        this.restTemplate = new RestTemplate();
        this.baseUrl = baseUrl;
    }

    /**
     * LIST peers: every registered peer together with its current status.
     *
     * @return the list of peers with their status
     */
    public List<PeerStatusResponse> listPeers() {
        String url = baseUrl + BASE_PATH;

        ResponseEntity<List<PeerStatusResponse>> response = restTemplate.exchange(
                url,
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<List<PeerStatusResponse>>() {}
        );

        return response.getBody();
    }

    /**
     * FIND peer: a single peer (by line id) with its current status.
     *
     * @param lineID the id of the production line / peer
     * @return the peer with its status
     */
    public PeerStatusResponse findPeer(int lineID) {
        String url = baseUrl + BASE_PATH + "/" + lineID;

        ResponseEntity<PeerStatusResponse> response = restTemplate.exchange(
                url,
                HttpMethod.GET,
                null,
                PeerStatusResponse.class
        );

        return response.getBody();
    }

    /**
     * FIND averages: the averages produced by a line, optionally bounded by a
     * time window. When {@code from} or {@code to} is {@code null} the server
     * falls back to its default window (the last minute).
     *
     * @param lineID the id of the production line / peer
     * @param from   lower bound timestamp (epoch millis) or {@code null}
     * @param to     upper bound timestamp (epoch millis) or {@code null}
     * @return the list of averages
     */
    public List<AverageMessage> findAverages(int lineID, Long from, Long to) {
        UriComponentsBuilder builder = UriComponentsBuilder
                .fromUriString(baseUrl + BASE_PATH + "/" + lineID + "/measurements");

        if (from != null) {
            builder.queryParam("from", from);
        }
        if (to != null) {
            builder.queryParam("to", to);
        }

        String url = builder.toUriString();

        ResponseEntity<List<AverageMessage>> response = restTemplate.exchange(
                url,
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<List<AverageMessage>>() {}
        );

        return response.getBody();
    }
}
