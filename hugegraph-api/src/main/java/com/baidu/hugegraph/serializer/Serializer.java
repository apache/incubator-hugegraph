package com.baidu.hugegraph.serializer;

import java.util.List;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public interface Serializer {

    public String writeVertex(Vertex v);

    public String writeVertices(List<Vertex> vertices);

    public String writeEdge(Edge e);

    public String writeEdges(List<Edge> edges);
}
