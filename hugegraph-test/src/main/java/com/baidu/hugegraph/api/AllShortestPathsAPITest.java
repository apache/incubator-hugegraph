package com.baidu.hugegraph.api;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;


public class AllShortestPathsAPITest extends BaseApiTest {
    public static String path = "graphs/hugegraph/traversers/allshortestpaths";
    @Before
    public void prepareSchema() {
        BaseApiTest.initPropertyKey();
        BaseApiTest.initVertexLabel();
        BaseApiTest.initEdgeLabel();
        BaseApiTest.initVertex();
        BaseApiTest.initEdge();
    }

    @Test
    public void get() {
        Map<String, String> name2Ids = getAllName2VertexIds();
        String markoId = name2Ids.get("marko");
        String vadasId=  name2Ids.get("vadas");
        Map<String, Object> map = ImmutableMap.of("source", "\""+ markoId+"\"",
                                                  "target", "\""+vadasId+"\"",
                                                  "max_depth", 100);
        assertResponseStatus(200, client().get(path, map));
    }
}
