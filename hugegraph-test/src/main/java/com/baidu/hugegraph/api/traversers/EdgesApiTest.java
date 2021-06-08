package com.baidu.hugegraph.api.traversers;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.api.BaseApiTest;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;


public class EdgesApiTest extends BaseApiTest {

    final static String EDGE_PATH = "graphs/hugegraph/traversers/edges";
    final static String SHARES_PATH = "graphs/hugegraph/traversers"
                                      + "/edges/shards";
    final static String SCAN_PATH = "graphs/hugegraph/traversers/edges/scan";

    @Before
    public void prepareSchema() {
        BaseApiTest.initPropertyKey();
        BaseApiTest.initVertexLabel();
        BaseApiTest.initEdgeLabel();
        BaseApiTest.initVertex();
        BaseApiTest.initEdge();
    }

    @Test
    public void testList() {
        Map<String, String> name2Ids = listAllVertexName2Ids();
        final String edgeGetPath = "graphs/hugegraph/graph/edges";
        String vadasId = name2Ids.get("vadas");
        Response r = client().get(edgeGetPath,
                                  ImmutableMap.of("vertex_id",
                                                  id2Json(vadasId),
                                                  "direction",
                                                  "IN"));
        String respBody = assertResponseStatus(200, r);
        List<Map> edges = assertJsonContains(respBody, "edges");
        assertNotNull(edges);
        assertFalse(edges.isEmpty());
        String edgeId = assertMapContains(edges.get(0), "id");
        assertNotNull(edgeId);

        r = client().get(EDGE_PATH,
                         ImmutableMultimap.of("ids", edgeId));
        assertResponseStatus(200, r);
    }

    @Test
    public void testShareAndScan() {
        Response r = client().get(SHARES_PATH, ImmutableMap.of("split_size",
                                                               1048576));
        String respBody = assertResponseStatus(200, r);
        List<Map> shards = assertJsonContains(respBody, "shards");
        assertNotNull(shards);
        assertFalse(shards.isEmpty());
        String start = assertMapContains(shards.get(0), "start");
        String end = assertMapContains(shards.get(0), "end");
        r = client().get(SCAN_PATH, ImmutableMap.of("start", start,
                                                    "end", end));
        respBody = assertResponseStatus(200, r);
        Map<String, Object> entity2 = parseMap(respBody);
        assertNotNull(entity2);
        assertFalse(entity2.isEmpty());
    }
}
