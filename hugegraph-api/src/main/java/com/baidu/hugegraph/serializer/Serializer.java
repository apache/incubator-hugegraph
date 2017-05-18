package com.baidu.hugegraph.serializer;

import java.util.List;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.baidu.hugegraph.schema.HugeEdgeLabel;
import com.baidu.hugegraph.schema.HugePropertyKey;
import com.baidu.hugegraph.schema.HugeVertexLabel;
import com.baidu.hugegraph.type.schema.EdgeLabel;
import com.baidu.hugegraph.type.schema.PropertyKey;
import com.baidu.hugegraph.type.schema.VertexLabel;

public interface Serializer {

    public String writePropertyKey(PropertyKey propertyKey);

    public String writePropertyKeys(List<HugePropertyKey> propertyKeys);

    public String writeVertexLabel(VertexLabel vertexLabel);

    public String writeVertexLabels(List<HugeVertexLabel> vertexLabels);

    public String writeEdgeLabel(EdgeLabel edgeLabel);

    public String writeEdgeLabels(List<HugeEdgeLabel> edgeLabels);

    public String writeVertex(Vertex v);

    public String writeVertices(List<Vertex> vertices);

    public String writeEdge(Edge e);

    public String writeEdges(List<Edge> edges);
}
