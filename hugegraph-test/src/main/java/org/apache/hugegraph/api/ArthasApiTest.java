package org.apache.hugegraph.api;

import org.junit.Before;
import org.junit.Test;

import jakarta.ws.rs.core.Response;

public class ArthasApiTest extends BaseApiTest {

    private static final String ARTHAS_START_PATH = "/arthas";
    private static final String ARTHAS_API_BASE_URL = "http://127.0.0.1:8561";
    private static final String ARTHAS_API_PATH = "/api";

    @Before
    public void testArthasStart() {
        Response r = client().get(ARTHAS_START_PATH);
        assertResponseStatus(200, r);
    }

    @Test
    public void testArthasApi() {

        String body = "{\n" +
                      "  \"action\": \"exec\",\n" +
                      "  \"requestId\": \"req112\",\n" +
                      "  \"sessionId\": \"\",\n" +
                      "  \"consumerId\": \"955dbd1325334a84972b0f3ac19de4f7_2\",\n" +
                      "  \"command\": \"version\",\n" +
                      "  \"execTimeout\": \"10000\"\n" +
                      "}";
        RestClient arthasApiClient = new RestClient(ARTHAS_API_BASE_URL);
        Response r = arthasApiClient.post(ARTHAS_API_PATH, body);
        String result = assertResponseStatus(200, r);
        assertJsonContains(result, "state");
        assertJsonContains(result, "requestId");
        assertJsonContains(result, "sessionId");
        assertJsonContains(result, "body");
    }
}
