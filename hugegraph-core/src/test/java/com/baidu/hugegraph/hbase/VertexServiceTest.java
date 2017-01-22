/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package com.baidu.hugegraph.hbase;

import java.util.Iterator;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import com.baidu.hugegraph.structure.HugeGraph;
import com.baidu.hugegraph.structure.HugeVertex;

/**
 * Created by zhangsuochao on 17/2/9.
 */
public class VertexServiceTest {

    @Test
    public void testQueryByLabel() {
        HugeGraph graph = HugeGraph.open(null);
        VertexService vertexService = new VertexService(graph);
        Iterator<Vertex> it = vertexService.vertices("Book");
        while (it.hasNext()) {
            HugeVertex v = (HugeVertex) it.next();
            Map<String, Object> properties = v.getProperties();
            System.out.println("id:" + v.id());
            System.out.println("label:" + v.label());
            for (String k : properties.keySet()) {
                System.out.println(k + ":" + properties.get(k));
            }

        }
    }
}
