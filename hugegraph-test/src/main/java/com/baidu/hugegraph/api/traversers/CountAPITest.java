package com.baidu.hugegraph.api.traversers;

import static com.baidu.hugegraph.testutil.Assert.assertEquals;

import javax.ws.rs.core.Response;

import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.api.BaseApiTest;

public class CountApiTest extends BaseApiTest {

    public static String path = "graphs/hugegraph/traversers/count";

    @Before
    public void prepareSchema() {
        BaseApiTest.initPropertyKey();
        BaseApiTest.initVertexLabel();
        BaseApiTest.initEdgeLabel();
        BaseApiTest.initVertex();
        BaseApiTest.initEdge();
    }

    @Test
    public void testCount() {
        String markoId = listAllVertexName2Ids().get("marko");
        String reqBody = String.format("{ "
                                       + "\"source\": \"%s\", "
                                       + "\"steps\": [{ "
                                       + "  \"labels\": [],"
                                       + "  \"degree\": 100,"
                                       + "  \"skip_degree\": 100},"
                                       + "  { "
                                       + "  \"labels\": [],"
                                       + "  \"degree\": 100,"
                                       + "  \"skip_degree\": 100}, "
                                       + "  { "
                                       + "  \"labels\": [],"
                                       + "  \"degree\": 100,"
                                       + "  \"skip_degree\": 100}]}", markoId);
        Response r = client().post(path, reqBody);
        String content = assertResponseStatus(200, r);
        Integer count = assertJsonContains(content, "count");
        assertEquals(3, count);
    }
}
