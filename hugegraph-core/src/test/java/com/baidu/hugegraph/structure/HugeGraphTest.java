/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package com.baidu.hugegraph.structure;

import java.util.Iterator;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhangsuochao on 17/2/8.
 */
public class HugeGraphTest {

    private static final Logger logger = LoggerFactory.getLogger(HugeGraphTest.class);
    //    @Test
    //    public void testAddVertex(){
    //        HugeGraph g = HugeGraph.open();
    //        for(int i=20;i<30;i++){
    //            g.addVertex(T.id,i,T.label,"Book","name","book"+i,"pages",20+i);
    //        }
    //
    //    }

    //    @Test
    //    public void testQueryVertex(){
    //        HugeGraph g = HugeGraph.open();
    //        Iterator<Vertex> it = g.vertices();
    //        while (it.hasNext()){
    //            HugeVertex v = (HugeVertex)it.next();
    //            Map<String,Object> properties = v.getProperties();
    //            System.out.println("id:"+v.id);
    //            System.out.println("label:"+v.label);
    //            for (String k:properties.keySet()){
    //                System.out.println(k+":"+properties.get(k));
    //            }
    //
    //        }
    //    }

    //    @Test
    public void testQueryVerteice() {
        HugeGraph graph = HugeGraph.open(null);

        Iterator<Vertex> vertices = graph.vertices();
        while (vertices.hasNext()) {
            Vertex v = vertices.next();
            Object o = v.value("test");
            System.out.println(o.toString());
        }
        //        assertTrue(IteratorUtils.stream(graph.edges()).allMatch(e -> e.value("data").equals("test")));
        //        Iterator<Edge> edges = g.edges("67b1f324-170b-419d-8ad3-a3879858c2ad");
        //        while (edges.hasNext()){
        //            HugeEdge he = (HugeEdge) edges.next();
        //
        //            logger.info("id:{},label:{},fromId:{},toId:{}" ,he.id(),he.label(),he.getVertex(Direction.OUT)
        // .id(),he
        //                    .getVertex(Direction.IN).id());
        //
        //        }
    }

    @Test
    public void testQueryEdges() {
        HugeGraph graph = HugeGraph.open(null);
        Iterator<Edge> edges = graph.edges();
        while (edges.hasNext()) {
            HugeEdge hugeEdge = (HugeEdge) edges.next();
            HugeVertex outVertex = (HugeVertex) hugeEdge.outVertex();
            HugeVertex inVertex = (HugeVertex) hugeEdge.inVertex();
            System.out.println(outVertex.property("name").value() + "-->" + hugeEdge.label() + "-->" + inVertex.property
                    ("name").value());
        }
    }

}
