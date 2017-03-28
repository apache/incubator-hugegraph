package com.baidu.hugegraph2.backend.serializer;

import com.baidu.hugegraph2.HugeGraph;
import com.baidu.hugegraph2.backend.id.Id;
import com.baidu.hugegraph2.backend.id.IdGenerator;
import com.baidu.hugegraph2.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph2.backend.store.BackendEntry;
import com.baidu.hugegraph2.structure.HugeEdge;
import com.baidu.hugegraph2.structure.HugeProperty;
import com.baidu.hugegraph2.structure.HugeVertex;
import com.baidu.hugegraph2.type.HugeTypes;
import com.baidu.hugegraph2.type.define.HugeKeys;
import com.baidu.hugegraph2.type.schema.EdgeLabel;
import com.baidu.hugegraph2.type.schema.PropertyKey;
import com.baidu.hugegraph2.type.schema.VertexLabel;

public class TextSerializer extends AbstractSerializer {

    private static final String COLUME_SPLITOR = SplicingIdGenerator.NAME_SPLITOR;
    private static final String VALUE_SPLITOR = "\u0003";

    public TextSerializer(final HugeGraph graph) {
        super(graph);
    }

    @Override
    public BackendEntry newBackendEntry(Id id) {
        return new TextBackendEntry(id);
    }

    protected String formatPropertyName(HugeProperty<?> prop) {
        return String.format("%s%s%s",
                prop.type().name(),
                COLUME_SPLITOR,
                prop.key());
    }

    protected String formatPropertyValue(HugeProperty<?> prop) {
        Object value = prop.value();
        assert value instanceof String;
        return value.toString();
    }

    protected String formatEdgeName(HugeEdge edge) {
        // type + edge-label-name + sortKeys + targetVertex
        StringBuilder sb = new StringBuilder(256);
        sb.append(edge.type().name());
        sb.append(COLUME_SPLITOR);
        sb.append(edge.label());
        sb.append(COLUME_SPLITOR);
        sb.append(edge.name());
        sb.append(COLUME_SPLITOR);
        sb.append(edge.otherVertex().id().asString());
        return sb.toString();
    }

    protected String formatEdgeValue(HugeEdge edge) {
        StringBuilder sb = new StringBuilder(256 * edge.getProperties().size());
        // edge id
        sb.append(edge.id().asString());
        // edge properties
        for (HugeProperty<?> property : edge.getProperties().values()) {
            sb.append(VALUE_SPLITOR);
            sb.append(property.key());
            sb.append(VALUE_SPLITOR);
            // TODO: property value is non-string
            sb.append(property.value().toString());
        }
        return sb.toString();
    }

    protected HugeEdge parseEdge(String name, String value, HugeVertex vertex) {
        String[] colParts = name.split(COLUME_SPLITOR);
        String[] valParts = value.split(VALUE_SPLITOR);

        boolean isOutEdge = colParts[0].equals(HugeTypes.EDGE_OUT.name());
        EdgeLabel label = this.graph.openSchemaManager().edgeLabel(colParts[1]);

        // TODO: how to construct targetVertex with id
        Id otherVertexId = IdGenerator.generate(colParts[3]);
        HugeVertex otherVertex = new HugeVertex(this.graph, otherVertexId, null);

        Id id = IdGenerator.generate(valParts[0]);

        HugeEdge edge = new HugeEdge(this.graph, id, label);

        if (isOutEdge) {
            edge.targetVertex(otherVertex);
            vertex.addOutEdge(edge);
        }
        else {
            edge.sourceVertex(otherVertex);
            vertex.addInEdge(edge);
        }

        for (int i = 1; i < valParts.length; i += 2) {
            edge.property(valParts[i], valParts[i + 1]);
        }

        return edge;
    }

    protected void parseColumn(String name, String value, HugeVertex vertex) {
        // column name
        String[] colParts = name.split(COLUME_SPLITOR);
        // property
        if (colParts[0].equals(HugeTypes.VERTEX_PROPERTY.name())) {
            vertex.property(colParts[1], value);
        }
        // edge
        else if (colParts[0].equals(HugeTypes.EDGE_OUT.name())
                || colParts[0].equals(HugeTypes.EDGE_IN.name())) {
            this.parseEdge(name, value, vertex);
        }
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
        for (HugeEdge edge : vertex.getEdges()) {
            // TODO: this.addEntry(v.id(), "edge:" + edge.colume(), edge);
            entry.column(this.formatEdgeName(edge),
                    this.formatEdgeValue(edge));
        }

        // test readVertex
//        System.out.println("writeVertex:" + entry);
//        HugeVertex v = readVertex(entry);
//        System.out.println("readVertex:" + v);

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

        // parse all properties or edges of a Vertex
        for (String name : entry.columnNames()) {
            this.parseColumn(name, entry.column(name), vertex);
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
