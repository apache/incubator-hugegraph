package com.baidu.hugegraph.api;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Map;

import javax.ws.rs.core.Response;

import org.junit.Before;
import org.junit.Test;

public class FusiformSimilarityAPITest extends BaseApiTest {
    final static String path = "graphs/hugegraph/traversers/fusiformsimilarity";

    @Before
    public void prepareSchema() {
        BaseApiTest.initPropertyKey();
        BaseApiTest.initVertexLabel();
        BaseApiTest.initEdgeLabel();
        BaseApiTest.initVertex();
        BaseApiTest.initEdge();
    }

    @Test
    public void post() {
        Response r = client().post(path, "{\n" +
                                         "    \"sources\":{\n" +
                                         "        \"ids\":[],\n" +
                                         "        \"label\": \"person\",\n" +
                                         "        \"properties\": {\n" +
                                         "        }\n" +
                                         "    },\n" +
                                         "    \"label\":\"created\",\n" +
                                         "    \"direction\":\"OUT\",\n" +
                                         "    \"min_neighbors\":1,\n" +
                                         "    \"alpha\":1,\n" +
                                         "    \"min_similars\":1,\n" +
                                         "    \"top\":0,\n" +
                                         "    \"group_property\":\"city\",\n" +
                                         "    \"min_groups\":2,\n" +
                                         "    \"max_degree\": 10000,\n" +
                                         "    \"capacity\": -1,\n" +
                                         "    \"limit\": -1,\n" +
                                         "    \"with_intermediary\": false,\n" +
                                         "    \"with_vertex\":true\n" +
                                         "}");
        assertEquals(200, r.getStatus());

        Map<String, Object> entity = r.readEntity(Map.class);
        assertNotNull(entity);
    }
}
