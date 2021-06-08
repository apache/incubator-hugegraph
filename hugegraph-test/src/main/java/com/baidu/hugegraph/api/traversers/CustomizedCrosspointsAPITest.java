package com.baidu.hugegraph.api.traversers;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.api.BaseApiTest;

public class CustomizedCrosspointsApiTest extends BaseApiTest {

    public static String path = "graphs/hugegraph/traversers/customizedcrosspoints";

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
        Map<String, String> name2Ids = listAllVertexName2Ids();
        String markoId = name2Ids.get("marko");
        String rippleId = name2Ids.get("ripple");
        String reqBody = String.format("{ "
                                       + "\"sources\":{ "
                                       + "  \"ids\":[\"%s\",\"%s\"]}, "
                                       + "\"path_patterns\":[{ "
                                       + "  \"steps\":[{ "
                                       + "    \"direction\":\"BOTH\","
                                       + "    \"labels\":[], "
                                       + "    \"degree\":-1}]}], "
                                       + "\"with_path\":true, "
                                       + "\"with_vertex\":true, "
                                       + "\"capacity\":-1, "
                                       + "\"limit\":-1}", markoId, rippleId);
        Response r = client().post(path, reqBody);
        String respBody = assertResponseStatus(200, r);
        List<Object> paths = assertJsonContains(respBody, "paths");
        assertEquals(2, paths.size());
    }
}
