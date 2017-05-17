package com.baidu.hugegraph.api;

import javax.ws.rs.NotFoundException;

import org.apache.tinkerpop.gremlin.structure.Graph;

import com.baidu.hugegraph.core.GraphManager;

public class API {

    public static Graph graph(GraphManager manager, String graph) {
        Graph g = manager.graph(graph);
        if (g == null) {
            String msg = String.format( "Not found graph '%s'", graph);
            throw new NotFoundException(msg);
        }
        return g;
    }
}
