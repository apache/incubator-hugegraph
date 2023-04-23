package com.baidu.hugegraph.store.meta;


import org.junit.Assert;
// import org.junit.Test;

public class GraphManagerTest {
    // @Test
    public void testCloneGraph(){
        Graph graph = new Graph();
        graph.setGraphName("test1");



        Graph graph1 = graph.clone();

        Assert.assertTrue(graph!=graph1);

        Assert.assertEquals(graph.getGraphName(), graph1.getGraphName());
        graph1.setGraphName("test4");


        Assert.assertNotEquals(graph.getGraphName(), graph1.getGraphName());

        Assert.assertEquals(graph.getGraphName(), "test1");

    }

}
