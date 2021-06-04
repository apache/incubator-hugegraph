package com.baidu.hugegraph.api;

import static com.baidu.hugegraph.testutil.Assert.assertEquals;

import java.util.Map;

import javax.ws.rs.core.Response;

import org.junit.Before;
import org.junit.Test;

public class CountAPITest extends BaseApiTest {
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
    public void count() {
        String markoId = getAllName2VertexIds().get("marko");
        Response r = client().post(path, "{\n" +
                                         "\t\"source\": \""+markoId+"\",\n" +
                                         "\t\"steps\": [\n" +
                                         "\t\t{\n" +
                                         "\t\t\t\"labels\": [\n" +
                                         "\t\t\t ],\n" +
                                         "\t\t\t \"degree\": 100,\n" +
                                         "\t\t\t \"skip_degree\": 100\n" +
                                         "\t\t},\n" +
                                         "\t\t\t{\n" +
                                         "\t\t\t\"labels\": [\n" +
                                         "\t\t\t ],\n" +
                                         "\t\t\t \"degree\": 100,\n" +
                                         "\t\t\t \"skip_degree\": 100\n" +
                                         "\t\t}\t\n" +
                                         "\t\t\n" +
                                         "\t]\n" +
                                         "\t\n" +
                                         "}");

        assertEquals(200, r.getStatus());
        Integer count = (Integer) r.readEntity(Map.class).get("count");
        assertEquals(5, count);

    }
}
