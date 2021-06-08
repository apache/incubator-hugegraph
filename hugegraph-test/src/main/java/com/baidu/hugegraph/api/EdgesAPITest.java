package com.baidu.hugegraph.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.ws.rs.core.Response;

import org.glassfish.grizzly.utils.Pair;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;


public class EdgesAPITest extends BaseApiTest {
    public final static String EDGE_PATH = "graphs/hugegraph/traversers/edges";
    public final static String SHARES_PATH = "graphs/hugegraph/traversers" +
                                            "/edges/shards";
    public final static String SCAN_PATH = "graphs/hugegraph/traversers/edges/scan";

    @Before
    public void prepareSchema() {
        BaseApiTest.initPropertyKey();
        BaseApiTest.initVertexLabel();
        BaseApiTest.initEdgeLabel();
        BaseApiTest.initVertex();
        BaseApiTest.initEdge();
    }

    @Test
    public void list() {
        Map<String, String> name2Ids = getAllName2VertexIds();
        final String edgeGetPath = "graphs/hugegraph/graph/edges";
        String vadasId = name2Ids.get("vadas");
        Response r = client().get(edgeGetPath, ImmutableMap.of("vertex_id",
                                                               "\"" + vadasId + "\"",
                                                               "direction",
                                                               "IN"));
        assertEquals(200, r.getStatus());
        Map entity = r.readEntity(Map.class);
        List<Map> edges = (List<Map>) entity.get("edges");
        assertNotNull(edges);
        Optional<Map> data = edges.stream().findFirst();
        assertEquals(true, data.isPresent());
        String edgeId = data.get().get("id").toString();
        assertNotNull(edgeId);

        r = client().get(EDGE_PATH,
                         ImmutableList.of(new Pair<String, Object>("ids",
                                                                edgeId)).iterator());
        assertEquals(200, r.getStatus());
    }

    @Test
    public void shareAndScan() {
        Response r = client().get(SHARES_PATH, ImmutableMap.of("split_size",
                                                               1048576));
        assertEquals(200, r.getStatus());
        Map entity = r.readEntity(Map.class);
        assertNotNull(entity);

        List<Map> shards = (List<Map>) entity.get("shards");
        assertNotNull(shards);
        assertEquals(false, shards.isEmpty());
        String start = shards.get(0).get("start").toString();
        String end = shards.get(0).get("end").toString();

        r = client().get(SCAN_PATH, ImmutableMap.of("start", start,
                                                          "end", end));
        assertEquals(200, r.getStatus());
        Map<String, Object> entity2 = r.readEntity(Map.class);
        assertNotNull(entity2);
        assertEquals(false, entity2.isEmpty());
    }
}
