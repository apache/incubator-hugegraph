package com.baidu.hugegraph.api.traversers;

import static com.baidu.hugegraph.testutil.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.api.BaseApiTest;
import com.google.common.collect.ImmutableMap;

public class CrosspointsAPITest extends BaseApiTest {

    public static String path = "graphs/hugegraph/traversers/crosspoints";

    @Before
    public void prepareSchema() {
        BaseApiTest.initPropertyKey();
        BaseApiTest.initVertexLabel();
        BaseApiTest.initEdgeLabel();
        BaseApiTest.initVertex();
        BaseApiTest.initEdge();
    }

    @Test
    public void testGet() {
        Map<String, String> name2Ids = listAllVertexName2Ids();
        String markoId = name2Ids.get("marko");
        String vadasId = name2Ids.get("vadas");
        Map<String, Object> params = ImmutableMap.of("source",
                                                     id2Json(markoId),
                                                     "target",
                                                     id2Json(vadasId),
                                                     "max_depth", 1000);
        Response r = client().get(path, params);
        String respBody = assertResponseStatus(200, r);
        List<Map<String, Object>> crosspoints = assertJsonContains(respBody,
                                                                   "crosspoints");
        assertEquals(2, crosspoints.size());
    }
}
