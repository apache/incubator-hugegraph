package com.baidu.hugegraph.api;

import java.util.Map;

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

    public static Object[] properties(Map<String, String> properties) {
        Object[] list = new Object[properties.size() * 2];
        int i = 0;
        for (Map.Entry<String, String> prop : properties.entrySet()) {
            list[i++] = prop.getKey();
            list[i++] = prop.getValue();
        }
        return list;
    }

    public static void checkExists(Object object, String name) {
        if (object == null) {
            String msg = String.format("Not found '%s'", name);
            throw new NotFoundException(msg);
        }
    }
}
