package com.baidu.hugegraph.api.traversers;

import static org.junit.Assert.assertEquals;

import java.util.Map;

import javax.ws.rs.core.Response;

import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.api.BaseApiTest;
import com.google.common.collect.ImmutableMap;

public class JaccardSimilarityApiTest extends BaseApiTest {

    final static String path = "graphs/hugegraph/traversers/jaccardsimilarity";

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
        String peterId = name2Ids.get("peter");
        Response r = client().get(path, ImmutableMap.of("vertex",
                                                        id2Json(markoId),
                                                        "other",
                                                        id2Json(peterId)));
        String content = assertResponseStatus(200, r);
        Double jaccardSimilarity = assertJsonContains(content,
                                                      "jaccard_similarity");
        assertEquals(0.25, jaccardSimilarity.doubleValue(), 0.0001);
    }

    @Test
    public void testPost() {
        Map<String, String> name2Ids = listAllVertexName2Ids();
        String markoId = name2Ids.get("marko");
        String rippleId = name2Ids.get("ripple");
        String peterId = name2Ids.get("peter");
        String jsonId = name2Ids.get("josh");
        String reqBody = String.format("{ "
                                       + "\"vertex\": \"%s\", "
                                       + "\"step\": { "
                                       + "  \"direction\": \"BOTH\", "
                                       + "  \"labels\": [], "
                                       + "  \"degree\": 10000, "
                                       + "  \"skip_degree\": 100000 }, "
                                       + "\"top\": 3}", markoId);
        Response r = client().post(path, reqBody);
        String respBody = assertResponseStatus(200, r);
        Map<String, Object> entity = parseMap(respBody);
        Double rippleJaccardSimilarity = assertMapContains(entity, rippleId);
        Double peterJaccardSimilarity = assertMapContains(entity, peterId);
        Double jsonJaccardSimilarity = assertMapContains(entity, jsonId);
        assertEquals(0.3333, rippleJaccardSimilarity.doubleValue(), 0.0001);
        assertEquals(0.25, peterJaccardSimilarity.doubleValue(), 0.0001);
        assertEquals(0.3333, jsonJaccardSimilarity.doubleValue(), 0.0001);
    }
}
