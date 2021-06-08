package com.baidu.hugegraph.api.traversers;

import static org.junit.Assert.assertNotNull;

import java.util.Map;

import javax.ws.rs.core.Response;

import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.api.BaseApiTest;

public class FusiformSimilarityApiTest extends BaseApiTest {

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
    public void testPost() {
        Response r = client().post(path, "{ "
                                         + "\"sources\":{ "
                                         + "  \"ids\":[], "
                                         + "  \"label\": \"person\", "
                                         + "  \"properties\": {}}, "
                                         + "\"label\":\"created\", "
                                         + "\"direction\":\"OUT\", "
                                         + "\"min_neighbors\":1, "
                                         + "\"alpha\":1, "
                                         + "\"min_similars\":1, "
                                         + "\"top\":0, "
                                         + "\"group_property\":\"city\", "
                                         + "\"min_groups\":2, "
                                         + "\"max_degree\": 10000, "
                                         + "\"capacity\": -1, "
                                         + "\"limit\": -1, "
                                         + "\"with_intermediary\": false, "
                                         + "\"with_vertex\":true}");
        String respBody = assertResponseStatus(200, r);
        Map<String, Object> entity = parseMap(respBody);
        assertNotNull(entity);
    }
}
