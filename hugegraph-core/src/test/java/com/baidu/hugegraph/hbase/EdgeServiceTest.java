/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package com.baidu.hugegraph.hbase;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import com.baidu.hugegraph.structure.HugeGraph;

/**
 * Created by zhangsuochao on 17/2/10.
 */
public class EdgeServiceTest {

    @Test
    public void testAddEdge() {
        HugeGraph graph = HugeGraph.open(null);
        EdgeService edgeService = new EdgeService(graph);

        Vertex v1 = graph.addVertex(T.id, 1, T.label, "Person", "name", "Tony", "age", 30);
        Vertex v2 = graph.addVertex(T.id, 2, T.label, "Person", "name", "Jim", "age", 28);
        Vertex v3 = graph.addVertex(T.id, 3, T.label, "Person", "name", "Piter", "age", 20);
        v1.addEdge("friends", v2, "years", 16, "weight", 0.5);
        v1.addEdge("friends", v3, "years", 10, "weight", 0.5);
        //        HugeEdge hugeEdge = new HugeEdge(graph,1,"friends",v1,v2);
        //        hugeEdge.setProperties("years",16,"weight",0.5);
        //        hugeEdge.setCreatedAt(System.currentTimeMillis());
        //        hugeEdge.setUpdatedAt(System.currentTimeMillis());
        //        edgeService.addEdge(hugeEdge);

    }
}
