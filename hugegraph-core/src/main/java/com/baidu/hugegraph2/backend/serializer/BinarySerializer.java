package com.baidu.hugegraph2.backend.serializer;

import java.nio.ByteBuffer;

import org.apache.tinkerpop.gremlin.structure.Edge;

import com.baidu.hugegraph2.HugeGraph;
import com.baidu.hugegraph2.backend.id.Id;
import com.baidu.hugegraph2.backend.store.BackendEntry;
import com.baidu.hugegraph2.backend.store.BackendEntry.BackendColumn;
import com.baidu.hugegraph2.structure.HugeProperty;
import com.baidu.hugegraph2.structure.HugeVertex;
import com.baidu.hugegraph2.type.HugeTypes;
import com.baidu.hugegraph2.type.define.HugeKeys;
import com.baidu.hugegraph2.type.schema.EdgeLabel;
import com.baidu.hugegraph2.type.schema.PropertyKey;
import com.baidu.hugegraph2.type.schema.VertexLabel;
import com.baidu.hugegraph2.util.StringEncoding;

public class BinarySerializer extends AbstractSerializer {

    public BinarySerializer(final HugeGraph graph) {
        super(graph);
    }

    @Override
    public BackendEntry newBackendEntry(Id id) {
        return new BinaryBackendEntry(id);
    }

    private BackendColumn formatLabel(VertexLabel vertexLabel) {
        BackendColumn col = new BackendColumn();
        col.name = new byte[]{ HugeKeys.LABEL.code() };
        // TODO: save label name or id?
        col.value = StringEncoding.encodeString(vertexLabel.name());
        return col;
    }

    private VertexLabel parseLabel(BackendColumn col) {
        String label = StringEncoding.decodeString(col.value);
        return this.graph.openSchemaManager().vertexLabel(label);
    }

    private byte[] formatPropertyName(HugeProperty<?> prop) {
        // with encoded bytes
        byte[] name = StringEncoding.encodeString(prop.key());
        ByteBuffer buffer = ByteBuffer.allocate(3 + name.length);
        buffer.put(prop.type().code());
        buffer.put(name); // writeString(name, buffer);
        return buffer.array();
    }

    private byte[] formatPropertyValue(HugeProperty<?> prop) {
        // with encoded bytes
        Object value = prop.value();
        // TODO: serialize any object, not only string
        return StringEncoding.encodeString(value.toString());
    }

    private BackendColumn formatProperty(HugeProperty<?> prop) {
        BackendColumn col = new BackendColumn();
        col.name = this.formatPropertyName(prop);
        col.value = this.formatPropertyValue(prop);
        return col;
    }

    private Object parsePropertyValue(byte[] bytes) {
        // TODO: deserialize any object, not only string
        return StringEncoding.decodeString(bytes);
    }

    private void parseColumn(BackendColumn col, HugeVertex vertex) {
        ByteBuffer buffer = ByteBuffer.wrap(col.name);
        byte type = buffer.get();
        // property
        if (type == HugeTypes.VERTEX_PROPERTY.code()) {
            String name = readStringFromRemaining(buffer);
            Object value = parsePropertyValue(col.value);
            vertex.property(name, value);
        }
        // edge
        else if (type == HugeTypes.EDGE_IN.code() ||
                type == HugeTypes.EDGE_OUT.code()) {
            // TODO: parse edge
            ;
        }
    }

    @Override
    public BackendEntry writeVertex(HugeVertex vertex) {
        BinaryBackendEntry entry = new BinaryBackendEntry(vertex.id());

        // label
        entry.column(this.formatLabel(vertex.vertexLabel()));

        // add all properties of a Vertex
        for (HugeProperty<?> prop : vertex.getProperties().values()) {
            entry.column(this.formatProperty(prop));
        }

        // add all edges of a Vertex
        for (Edge edge : vertex.getEdges()) {
            // TODO: format edge
        }

        return entry;
    }

    @Override
    public HugeVertex readVertex(BackendEntry bytesEntry) {
        assert bytesEntry instanceof BinaryBackendEntry;
        BinaryBackendEntry entry = (BinaryBackendEntry) bytesEntry;

        // label
        VertexLabel label = this.parseLabel(entry.column(HugeKeys.LABEL.code()));

        // id
        HugeVertex vertex = new HugeVertex(this.graph, entry.id(), label);

        // parse all properties and edges of a Vertex
        for (BackendColumn col : entry.columns()) {
            this.parseColumn(col, vertex);
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

    protected static void writeString(byte[] bytes, ByteBuffer buffer) {
        assert bytes.length < Short.MAX_VALUE;
        buffer.putShort((short) bytes.length);
        buffer.put(bytes);
    }

    protected static void writeString(String value, ByteBuffer buffer) {
        byte[] bytes = StringEncoding.encodeString(value);
        writeString(bytes, buffer);
    }

    protected static String readString(ByteBuffer buffer) {
        short length = buffer.getShort();
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return StringEncoding.decodeString(bytes);
    }

    protected static String readStringFromRemaining(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return StringEncoding.decodeString(bytes);
    }
}

