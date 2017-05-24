package com.baidu.hugegraph.core;

import java.util.List;

import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.core.FakeObjects.FakeVertex;

public class Utils {

    public static boolean containsId(List<Vertex> vertexes, Id id) {
        for (Vertex v : vertexes) {
            if (v.id().equals(id)) {
                return true;
            }
        }
        return false;
    }

    public static boolean contains(List<Vertex> vertexes,
            FakeVertex fakeVertex) {
        for (Vertex v : vertexes) {
            if (fakeVertex.equalsVertex(v)) {
                return true;
            }
        }
        return false;
    }
}
