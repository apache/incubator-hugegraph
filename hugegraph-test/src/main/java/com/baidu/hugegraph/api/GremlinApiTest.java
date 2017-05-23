package com.baidu.hugegraph.api;

import java.util.Map;

import javax.ws.rs.core.Response;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class GremlinApiTest extends BaseTest {

    private static String path = "/gremlin";

    @Test
    public void testPost() {
        String body = "{"
                + "\"gremlin\":\"g.V()\","
                + "\"bindings\":{},"
                + "\"language\":\"gremlin-groovy\","
                + "\"aliases\":{}}";
        Assert.assertEquals(200, client().post(path, body).getStatus());
    }

    @Test
    public void testGet() {
        Map<String, Object> params = ImmutableMap.of("gremlin", "g.V()");
        Response r = client().get(path, params);
        Assert.assertEquals(r.readEntity(String.class), 200, r.getStatus());
    }
}
