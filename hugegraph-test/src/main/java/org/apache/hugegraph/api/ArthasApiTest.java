package org.apache.hugegraph.api;

import org.junit.Test;

import jakarta.ws.rs.core.Response;

public class ArthasApiTest extends BaseApiTest {


    private static final String path = "/arthasstart";

    @Test
    public void testMetricsAll() {
        Response r = client().get(path);
        assertResponseStatus(200, r);
    }
}
