package com.baidu.hugegraph.serializer;

import java.io.ByteArrayOutputStream;
import java.util.List;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.type.schema.EdgeLabel;
import com.baidu.hugegraph.type.schema.IndexLabel;
import com.baidu.hugegraph.type.schema.PropertyKey;
import com.baidu.hugegraph.type.schema.VertexLabel;

public class JsonSerializer implements Serializer {

    private GraphSONWriter writer;

    public JsonSerializer(GraphSONWriter writer) {
        this.writer = writer;
    }

    private String writeObject(Object object) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            this.writer.writeObject(out, object);
        } catch (Exception e) {
            throw new HugeException(String.format(
                    "Failed to serialize %s",
                    object.getClass().getSimpleName()), e);
        }

        return out.toString();
    }

    private String writeList(String label, Object object) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            out.write(String.format("{\"%s\": ", label).getBytes());
            this.writer.writeObject(out, object);
            out.write("}".getBytes());
        } catch (Exception e) {
            throw new HugeException(String.format(
                    "Failed to serialize %s", label), e);
        }

        return out.toString();
    }

    @Override
    public String writePropertyKey(PropertyKey propertyKey) {
        return writeObject(propertyKey);
    }

    @Override
    public String writePropertyKeys(List<PropertyKey> propertyKeys) {
        return writeList("propertykeys", propertyKeys);
    }

    @Override
    public String writeVertexLabel(VertexLabel vertexLabel) {
        return writeObject(vertexLabel);
    }

    @Override
    public String writeVertexLabels(List<VertexLabel> vertexLabels) {
        return writeList("vertexlabels", vertexLabels);
    }

    @Override
    public String writeEdgeLabel(EdgeLabel edgeLabel) {
        return writeObject(edgeLabel);
    }

    @Override
    public String writeEdgeLabels(List<EdgeLabel> edgeLabels) {
        return writeList("edgelabels", edgeLabels);
    }

    @Override
    public String writeIndexlabel(IndexLabel indexLabel) {
        return writeObject(indexLabel);
    }

    @Override
    public String writeIndexlabels(List<IndexLabel> indexLabels) {
        return writeList("indexlabels", indexLabels);
    }

    @Override
    public String writeVertex(Vertex vertex) {
        return writeObject(vertex);
    }

    @Override
    public String writeVertices(List<Vertex> vertices) {
        return writeList("vertices", vertices);
    }

    @Override
    public String writeEdge(Edge edge) {
        return writeObject(edge);
    }

    @Override
    public String writeEdges(List<Edge> edges) {
        return writeList("edges", edges);
    }
}
