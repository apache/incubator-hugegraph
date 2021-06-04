package com.baidu.hugegraph.api;

import static com.baidu.hugegraph.testutil.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class CrosspointsAPITest extends BaseApiTest{
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
    public void get(){
        Map<String, String> name2Ids = getAllName2VertexIds();
        String markoId = name2Ids.get("marko");
        String vadasId = name2Ids.get("vadas");
        Map<String, Object> params = ImmutableMap.of("source",
                                                     "\"" + markoId + "\"",
                                                     "target",
                                                     "\"" + vadasId + "\"",
                                                     "max_depth", 1000);
        Response r = client().get(path, params);
        assertEquals(200, r.getStatus());
        Map<String, Object> data = r.readEntity(Map.class);
        List<Map<String, Object>> crosspoints =
                (List<Map<String, Object>>) data.get("crosspoints");
        assertEquals(2, crosspoints.size());
    }
}
