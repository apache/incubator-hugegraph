package com.baidu.hugegraph.api;

import org.junit.Before;
import org.junit.Test;

public class JaccardSimilarityAPITest extends BaseApiTest {
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
    public void get() {
    }

    @Test
    public void post() {
    }
}
