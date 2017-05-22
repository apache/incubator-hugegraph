package com.baidu.hugegraph.serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.schema.HugeEdgeLabel;
import com.baidu.hugegraph.schema.HugeIndexLabel;
import com.baidu.hugegraph.schema.HugePropertyKey;
import com.baidu.hugegraph.schema.HugeVertexLabel;
import com.baidu.hugegraph.type.schema.EdgeLabel;
import com.baidu.hugegraph.type.schema.IndexLabel;
import com.baidu.hugegraph.type.schema.PropertyKey;
import com.baidu.hugegraph.type.schema.VertexLabel;

public class JsonSerializer implements Serializer {

    private GraphSONWriter writer;

    public JsonSerializer(GraphSONWriter writer) {
        this.writer = writer;
    }

    @Override
    public String writePropertyKey(PropertyKey propertyKey) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            this.writer.writeObject(out, propertyKey);
        } catch (IOException e) {
            throw new HugeException("Failed to serialize property key", e);
        }

        return out.toString();
    }

    @Override
    public String writePropertyKeys(List<HugePropertyKey> propertyKeys) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            this.writer.writeObject(out, propertyKeys.iterator());
        } catch (IOException e) {
            throw new HugeException("Failed to serialize property keys", e);
        }
        return out.toString();
    }

    @Override
    public String writeVertexLabel(VertexLabel vertexLabel) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            this.writer.writeObject(out, vertexLabel);
        } catch (IOException e) {
            throw new HugeException("Failed to serialize vertex label", e);
        }

        return out.toString();
    }

    @Override
    public String writeVertexLabels(List<HugeVertexLabel> vertexLabels) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            this.writer.writeObject(out, vertexLabels.iterator());
        } catch (IOException e) {
            throw new HugeException("Failed to serialize vertex labels", e);
        }
        return out.toString();
    }

    @Override
    public String writeEdgeLabel(EdgeLabel edgeLabel) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            this.writer.writeObject(out, edgeLabel);
        } catch (IOException e) {
            throw new HugeException("Failed to serialize edge label", e);
        }

        return out.toString();
    }

    @Override
    public String writeEdgeLabels(List<HugeEdgeLabel> edgeLabels) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            this.writer.writeObject(out, edgeLabels.iterator());
        } catch (IOException e) {
            throw new HugeException("Failed to serialize edge labels", e);
        }
        return out.toString();
    }

    @Override
    public String writeIndexlabel(IndexLabel indexLabel) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            this.writer.writeObject(out, indexLabel);
        } catch (IOException e) {
            throw new HugeException("Failed to serialize index label", e);
        }

        return out.toString();
    }

    @Override
    public String writeIndexlabels(List<HugeIndexLabel> indexLabels) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            this.writer.writeObject(out, indexLabels.iterator());
        } catch (IOException e) {
            throw new HugeException("Failed to serialize index labels", e);
        }
        return out.toString();
    }

    @Override
    public String writeVertex(Vertex v) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            this.writer.writeVertex(out, v);
        } catch (IOException e) {
            throw new HugeException("Failed to serialize vertex", e);
        }
        return out.toString();
    }

    @Override
    public String writeVertices(List<Vertex> vertices) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            this.writer.writeVertices(out, vertices.iterator());
        } catch (IOException e) {
            throw new HugeException("Failed to serialize vertices", e);
        }
        // NOTE: please ensure the writer wrapAdjacencyList(true)
        return out.toString();
    }

    @Override
    public String writeEdge(Edge edge) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            this.writer.writeEdge(out, edge);
        } catch (IOException e) {
            throw new HugeException("Failed to serialize edge", e);
        }
        return out.toString();
    }

    @Override
    public String writeEdges(List<Edge> edges) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            out.write("{\"edges\": [".getBytes());
            for (Edge edge : edges) {
                this.writer.writeEdge(out, edge);
                out.write(",".getBytes());
            }
            out.write("]}".getBytes());
        } catch (IOException e) {
            throw new HugeException("Failed to serialize edges", e);
        }
        return out.toString();
    }
}
