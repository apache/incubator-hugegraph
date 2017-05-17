package com.baidu.hugegraph.serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

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
        return out.toString();
    }
}
