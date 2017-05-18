package com.baidu.hugegraph.serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;

import com.baidu.hugegraph.HugeException;

public class JsonSerializer implements Serializer {

    private GraphSONWriter writer;

    public JsonSerializer(GraphSONWriter writer) {
        this.writer = writer;
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
