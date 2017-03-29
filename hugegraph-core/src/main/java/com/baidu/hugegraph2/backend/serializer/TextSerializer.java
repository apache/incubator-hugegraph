package com.baidu.hugegraph2.backend.serializer;

import java.util.Map;

import com.baidu.hugegraph2.HugeGraph;
import com.baidu.hugegraph2.backend.id.Id;
import com.baidu.hugegraph2.backend.id.IdGeneratorFactory;
import com.baidu.hugegraph2.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph2.backend.store.BackendEntry;
import com.baidu.hugegraph2.schema.HugeEdgeLabel;
import com.baidu.hugegraph2.schema.HugePropertyKey;
import com.baidu.hugegraph2.schema.HugeVertexLabel;
import com.baidu.hugegraph2.schema.SchemaElement;
import com.baidu.hugegraph2.structure.HugeEdge;
import com.baidu.hugegraph2.structure.HugeProperty;
import com.baidu.hugegraph2.structure.HugeVertex;
import com.baidu.hugegraph2.type.HugeTypes;
import com.baidu.hugegraph2.type.define.Cardinality;
import com.baidu.hugegraph2.type.define.DataType;
import com.baidu.hugegraph2.type.define.HugeKeys;
import com.baidu.hugegraph2.type.schema.EdgeLabel;
import com.baidu.hugegraph2.type.schema.PropertyKey;
import com.baidu.hugegraph2.type.schema.VertexLabel;
import com.google.gson.Gson;

public class TextSerializer extends AbstractSerializer {

    private static final String COLUME_SPLITOR = SplicingIdGenerator.NAME_SPLITOR;
    private static final String VALUE_SPLITOR = "\u0003";

    private static Gson gson = new Gson();

    public TextSerializer(final HugeGraph graph) {
        super(graph);
    }

    public static String toJson(Object object) {
        return gson.toJson(object);
    }

    public static <T> T fromJson(String json, Class<T> clazz) {
        return gson.fromJson(json, clazz);
    }

    @Override
    public BackendEntry newBackendEntry(Id id) {
        return new TextBackendEntry(id);
    }

    protected String formatSystemPropertyName(String name) {
        return String.format("%s%s%s",
                HugeTypes.SYS_PROPERTY.name(),
                COLUME_SPLITOR,
                name);
    }

    protected String formatSystemPropertyName(HugeKeys col) {
        return this.formatSystemPropertyName(col.string());
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

    // parse an edge from a column item
    protected HugeEdge parseEdge(String colName, String colValue, HugeVertex vertex) {
        String[] colParts = colName.split(COLUME_SPLITOR);
        String[] valParts = colValue.split(VALUE_SPLITOR);

        boolean isOutEdge = colParts[0].equals(HugeTypes.EDGE_OUT.name());
        EdgeLabel label = this.graph.openSchemaManager().edgeLabel(colParts[1]);

        // TODO: how to construct targetVertex with id
        Id otherVertexId = IdGeneratorFactory.generator().generate(colParts[3]);
        HugeVertex otherVertex = new HugeVertex(this.graph, otherVertexId, null);

        Id id = IdGeneratorFactory.generator().generate(valParts[0]);

        HugeEdge edge = new HugeEdge(this.graph, id, label);

        if (isOutEdge) {
            edge.targetVertex(otherVertex);
            vertex.addOutEdge(edge);
        } else {
            edge.sourceVertex(otherVertex);
            vertex.addInEdge(edge);
        }

        for (int i = 1; i < valParts.length; i += 2) {
            edge.property(valParts[i], valParts[i + 1]);
        }

        return edge;
    }

    protected void parseColumn(String colName, String colValue, HugeVertex vertex) {
        // column name
        String[] colParts = colName.split(COLUME_SPLITOR);
        // property
        if (colParts[0].equals(HugeTypes.VERTEX_PROPERTY.name())) {
            // TODO: currently we assume property value is a string
            vertex.property(colParts[1], colValue);
        }
        // edge
        else if (colParts[0].equals(HugeTypes.EDGE_OUT.name())
                || colParts[0].equals(HugeTypes.EDGE_IN.name())) {
            this.parseEdge(colName, colValue, vertex);
        }
    }

    @Override
    public BackendEntry writeVertex(HugeVertex vertex) {
        TextBackendEntry entry = new TextBackendEntry(vertex.id());

        // label
        entry.column(this.formatSystemPropertyName(HugeKeys.LABEL),
                vertex.label());

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
        String labelName = entry.column(this.formatSystemPropertyName(HugeKeys.LABEL));
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
        Id id = IdGeneratorFactory.generator().generate(vertexLabel);

        TextBackendEntry entry = new TextBackendEntry(id);
        entry.column(HugeKeys.NAME.string(), vertexLabel.name());
        entry.column(HugeKeys.PRIMARY_KEYS.string(), toJson(vertexLabel.primaryKeys().toArray()));
        writeProperties(vertexLabel, entry);
        return entry;
    }

    @Override
    public BackendEntry writeEdgeLabel(EdgeLabel edgeLabel) {
        Id id = IdGeneratorFactory.generator().generate(edgeLabel);

        TextBackendEntry entry = new TextBackendEntry(id);
        entry.column(HugeKeys.NAME.string(), edgeLabel.name());
        entry.column(HugeKeys.CARDINALITY.string(), toJson(edgeLabel.cardinality()));
        entry.column(HugeKeys.MULTIPLICITY.string(), toJson(edgeLabel.multiplicity()));
        entry.column(HugeKeys.LINKS.string(), toJson(edgeLabel.links().toArray()));
        entry.column(HugeKeys.SORT_KEYS.string(), toJson(edgeLabel.sortKeys().toArray()));
        writeProperties(edgeLabel, entry);
        return entry;
    }

    @Override
    public BackendEntry writePropertyKey(PropertyKey propertyKey) {
        Id id = IdGeneratorFactory.generator().generate(propertyKey);

        TextBackendEntry entry = new TextBackendEntry(id);
        entry.column(HugeKeys.NAME.string(), propertyKey.name());
        entry.column(HugeKeys.DATA_TYPE.string(), toJson(propertyKey.dataType()));
        entry.column(HugeKeys.CARDINALITY.string(), toJson(propertyKey.cardinality()));
        writeProperties(propertyKey, entry);
        return entry;
    }

    public void writeProperties(SchemaElement schemaElement, TextBackendEntry entry) {
        Map<String, PropertyKey> properties = schemaElement.properties();
        if (properties == null) {
            entry.column(HugeKeys.PROPERTIES.string(), toJson(""));
        } else {
            entry.column(HugeKeys.PROPERTIES.string(), toJson(properties.keySet().toArray()));
        }
    }

    @Override
    public VertexLabel readVertexLabel(BackendEntry entry) {
        if (entry == null) {
            return null;
        }

        entry = convertEntry(entry);
        assert entry instanceof TextBackendEntry;

        TextBackendEntry textEntry = (TextBackendEntry) entry;
        String name = textEntry.column(HugeKeys.NAME.string());
        String properties = textEntry.column(HugeKeys.PROPERTIES.string());
        String primarykeys = textEntry.column(HugeKeys.PRIMARY_KEYS.string());

        HugeVertexLabel vertexLabel = new HugeVertexLabel(name, this.graph.openSchemaTransaction());
        vertexLabel.properties(fromJson(properties, String[].class));
        vertexLabel.primaryKeys(fromJson(primarykeys, String[].class));

        return vertexLabel;
    }

    @Override
    public EdgeLabel readEdgeLabel(BackendEntry entry) {
        if (entry == null) {
            return null;
        }

        entry = convertEntry(entry);
        assert entry instanceof TextBackendEntry;

        TextBackendEntry textEntry = (TextBackendEntry) entry;
        String name = textEntry.column(HugeKeys.NAME.string());
        String sortKeys = textEntry.column(HugeKeys.SORT_KEYS.string());
        String links = textEntry.column(HugeKeys.LINKS.string());
        String properties = textEntry.column(HugeKeys.PROPERTIES.string());

        HugeEdgeLabel edgeLabel = new HugeEdgeLabel(name, this.graph.openSchemaTransaction());
        edgeLabel.properties(fromJson(properties, String[].class));
        edgeLabel.sortKeys(fromJson(sortKeys, String[].class));
        String[] linksArray = fromJson(links, String[].class);
        for (int i = 0; i < linksArray.length - 1; i += 2) {
            edgeLabel.link(linksArray[i], linksArray[i + 1]);
        }

        return edgeLabel;
    }

    @Override
    public PropertyKey readPropertyKey(BackendEntry entry) {
        if (entry == null) {
            return null;
        }

        entry = convertEntry(entry);

        assert entry instanceof TextBackendEntry;

        TextBackendEntry textEntry = (TextBackendEntry) entry;
        String name = textEntry.column(HugeKeys.NAME.string());
        String dataType = textEntry.column(HugeKeys.DATA_TYPE.string());
        String cardinality = textEntry.column(HugeKeys.CARDINALITY.string());
        String properties = textEntry.column(HugeKeys.PROPERTIES.string());

        HugePropertyKey propertyKey = new HugePropertyKey(name, this.graph.openSchemaTransaction());
        propertyKey.dataType(fromJson(dataType, DataType.class));
        propertyKey.cardinality(fromJson(cardinality, Cardinality.class));
        propertyKey.properties(fromJson(properties, String[].class));

        return propertyKey;
    }
}
