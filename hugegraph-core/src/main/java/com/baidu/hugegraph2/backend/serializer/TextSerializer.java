package com.baidu.hugegraph2.backend.serializer;

import org.apache.tinkerpop.gremlin.structure.Edge;

import com.baidu.hugegraph2.HugeGraph;
import com.baidu.hugegraph2.backend.id.Id;
import com.baidu.hugegraph2.backend.store.BackendEntry;
import com.baidu.hugegraph2.structure.HugeProperty;
import com.baidu.hugegraph2.structure.HugeVertex;
import com.baidu.hugegraph2.type.define.HugeKeys;
import com.baidu.hugegraph2.type.schema.EdgeLabel;
import com.baidu.hugegraph2.type.schema.PropertyKey;
import com.baidu.hugegraph2.type.schema.VertexLabel;

public class TextSerializer extends AbstractSerializer {

    public TextSerializer(final HugeGraph graph) {
        super(graph);
    }

    @Override
    public BackendEntry newBackendEntry(Id id) {
        return new TextBackendEntry(id);
    }

    public String formatPropertyName(HugeProperty<?> prop) {
        return String.format("%s:%s", prop.type().name(), prop.key());
    }

    public String formatPropertyValue(HugeProperty<?> prop) {
        Object value = prop.value();
        assert value instanceof String;
        return value.toString();
    }

    @Override
    public BackendEntry writeVertex(HugeVertex vertex) {
        TextBackendEntry entry = new TextBackendEntry(vertex.id());

        // label
        entry.column(HugeKeys.LABEL.string(), vertex.label());

        // add all properties of a Vertex
        for (HugeProperty<?> prop : vertex.getProperties().values()) {
            entry.column(this.formatPropertyName(prop),
                    this.formatPropertyValue(prop));
        }

        // add all edges of a Vertex
        for (Edge edge : vertex.getEdges()) {
            // TODO: this.addEntry(v.id(), "edge:" + edge.colume(), edge);
        }

        return entry;
    }

    @Override
    public HugeVertex readVertex(BackendEntry bytesEntry) {
        assert bytesEntry instanceof TextBackendEntry;
        TextBackendEntry entry = (TextBackendEntry) bytesEntry;

        // label
        String labelName = entry.column(HugeKeys.LABEL.string());
        VertexLabel label = this.graph.openSchemaManager().vertexLabel(labelName);

        // id
        HugeVertex vertex = new HugeVertex(this.graph, entry.id(), label);

        // parse all properties of a Vertex
        for (String name : entry.columnNames()) {
            vertex.property(name, entry.column(name));
        }

        // parse all edges of a Vertex
        for (Edge edge : vertex.getEdges()) {
            // TODO: vertex.addEdge();
        }

        return vertex;
    }

    @Override
    public BackendEntry writeVertexLabel(VertexLabel vertexLabel) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public BackendEntry writeEdgeLabel(EdgeLabel edgeLabel) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public BackendEntry writePropertyKey(PropertyKey propertyKey) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public VertexLabel readVertexLabel(BackendEntry entry) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public EdgeLabel readEdgeLabel(BackendEntry entry) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PropertyKey readPropertyKey(BackendEntry entry) {
        // TODO Auto-generated method stub
        return null;
    }

}
