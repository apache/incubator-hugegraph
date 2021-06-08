package com.baidu.hugegraph.api.traversers;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.api.BaseApiTest;
import com.google.common.collect.ImmutableMap;

public class AllShortestPathsApiTest extends BaseApiTest {

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
    public void testGet() {
        Map<String, String> name2Ids = listAllVertexName2Ids();
        String markoId = name2Ids.get("marko");
        String vadasId = name2Ids.get("vadas");
        Map<String, Object> entities = ImmutableMap.of("source",
                                                       id2Json(markoId),
                                                       "target",
                                                       id2Json(vadasId),
                                                       "max_depth", 100);
        assertResponseStatus(200, client().get(path, entities));
    }
}
