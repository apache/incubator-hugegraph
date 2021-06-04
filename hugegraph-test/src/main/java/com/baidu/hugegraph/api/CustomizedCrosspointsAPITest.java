package com.baidu.hugegraph.api;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.api.traversers.CustomizedCrosspointsAPI;

public class CustomizedCrosspointsAPITest extends BaseApiTest{
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
    public void post() {
        Map<String, String> name2Ids = getAllName2VertexIds();
        String markoId = name2Ids.get("marko");
        String rippleId = name2Ids.get("ripple");
        Response r = client().post(path, "{\n" +
                                         "    \"sources\":{\n" +
                                         "        \"ids\":[\n" +
                                         "            \"" + markoId + "\",\n" +
                                         "            \"" + rippleId + "\"\n" +
                                         "        ]\n" +
                                         "    },\n" +
                                         "    \"path_patterns\":[\n" +
                                         "        {\n" +
                                         "            \"steps\":[\n" +
                                         "                {\n" +
                                         "                    \"direction\":\"BOTH\"," +
                                         "                    \"labels\":[\n" +
                                         "                    ],\n" +
                                         "                    \"degree\":-1\n" +
                                         "                }\n" +
                                         "            ]\n" +
                                         "        }\n" +
                                         "    ],\n" +
                                         "    \"with_path\":true,\n" +
                                         "    \"with_vertex\":true,\n" +
                                         "    \"capacity\":-1,\n" +
                                         "    \"limit\":-1\n" +
                                         "}");

        assertEquals(200, r.getStatus());
        Map<String, Object> map = r.readEntity(Map.class);
        List<Object> paths = (List<Object>) map.get("paths");
        assertEquals(2, paths.size());
    }
}
