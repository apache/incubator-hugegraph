package com.baidu.hugegraph.backend.serializer;

import java.util.Collection;
import java.util.Map;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGeneratorFactory;
import com.baidu.hugegraph.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph.backend.query.IdQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.cassandra.CassandraBackendEntry;
import com.baidu.hugegraph.schema.HugeEdgeLabel;
import com.baidu.hugegraph.schema.HugeIndexLabel;
import com.baidu.hugegraph.schema.HugePropertyKey;
import com.baidu.hugegraph.schema.HugeVertexLabel;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeIndex;
import com.baidu.hugegraph.structure.HugeProperty;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.HugeTypes;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.define.DataType;
import com.baidu.hugegraph.type.define.Frequency;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.type.define.IndexType;
import com.baidu.hugegraph.type.schema.EdgeLabel;
import com.baidu.hugegraph.type.schema.IndexLabel;
import com.baidu.hugegraph.type.schema.PropertyKey;
import com.baidu.hugegraph.type.schema.VertexLabel;
import com.google.gson.Gson;

public class TextSerializer extends AbstractSerializer {

    private static final String COLUME_SPLITOR = SplicingIdGenerator.NAME_SPLITOR;
    private static final String VALUE_SPLITOR = "\u0004";

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

    @Override
    protected BackendEntry convertEntry(BackendEntry entry) {
        if (entry instanceof TextBackendEntry) {
            return entry;
        } else {
            TextBackendEntry text = new TextBackendEntry(entry.id());
            text.columns(entry.columns());
            return text;
        }
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
        // may be a single value or a list of values
        return toJson(prop.value());
    }

    protected void parseProperty(String colName, String colValue, HugeElement owner) {
        String[] colParts = colName.split(COLUME_SPLITOR);

        // get PropertyKey by PropertyKey name
        PropertyKey pkey = this.graph.schema().propertyKey(colParts[1]);

        // parse value
        Object value = fromJson(colValue, pkey.clazz());

        // set properties of vertex/edge
        if (pkey.cardinality() == Cardinality.SINGLE) {
            owner.property(pkey.name(), value);
        } else {
            if (value instanceof Collection) {
                for (Object v : (Collection<?>) value) {
                    owner.property(pkey.name(), v);
                }
            } else {
                assert false : "invalid value of non-sigle property";
            }
        }
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
            sb.append(this.formatPropertyName(property));
            sb.append(VALUE_SPLITOR);
            sb.append(this.formatPropertyValue(property));
        }
        return sb.toString();
    }

    // parse an edge from a column item
    protected void parseEdge(String colName, String colValue, HugeVertex vertex) {
        String[] colParts = colName.split(COLUME_SPLITOR);
        String[] valParts = colValue.split(VALUE_SPLITOR);

        boolean isOutEdge = colParts[0].equals(HugeTypes.EDGE_OUT.name());
        EdgeLabel label = this.graph.schema().edgeLabel(colParts[1]);

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

        // edge properties
        for (int i = 1; i < valParts.length; i += 2) {
            this.parseProperty(valParts[i], valParts[i + 1], edge);
        }
    }

    protected void parseColumn(String colName, String colValue, HugeVertex vertex) {
        // column name
        String type = colName.split(COLUME_SPLITOR, 2)[0];
        // property
        if (type.equals(HugeTypes.VERTEX_PROPERTY.name())) {
            this.parseProperty(colName, colValue, vertex);
        }
        // edge
        else if (type.equals(HugeTypes.EDGE_OUT.name())
                || type.equals(HugeTypes.EDGE_IN.name())) {
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
            entry.column(this.formatEdgeName(edge),
                    this.formatEdgeValue(edge));
        }

        return entry;
    }

    @Override
    public HugeVertex readVertex(BackendEntry bytesEntry) {
        if (bytesEntry == null) {
            return null;
        }
        bytesEntry = this.convertEntry(bytesEntry);
        assert bytesEntry instanceof TextBackendEntry;
        TextBackendEntry entry = (TextBackendEntry) bytesEntry;

        // label
        String labelName = entry.column(this.formatSystemPropertyName(HugeKeys.LABEL));
        VertexLabel label = this.graph.schema().vertexLabel(labelName);

        // id
        HugeVertex vertex = new HugeVertex(this.graph, entry.id(), label);

        // parse all properties or edges of a Vertex
        for (String name : entry.columnNames()) {
            this.parseColumn(name, entry.column(name), vertex);
        }

        return vertex;
    }

    @Override
    public TextBackendEntry writeId(HugeTypes type, Id id) {
        return new TextBackendEntry(id.prefixWith(type));
    }

    @Override
    public Query writeQuery(Query query) {
        if (SchemaElement.isSchema(query.resultType())
                && query instanceof IdQuery) {
            // serialize query id of schema
            IdQuery result = (IdQuery) query.clone();
            result.resetIds();
            for (Id id : query.ids()) {
                result.query(id.prefixWith(query.resultType()));
            }
            return result;
        }
        return query;
    }

    @Override
    public BackendEntry writeVertexLabel(VertexLabel vertexLabel) {
        Id id = IdGeneratorFactory.generator().generate(vertexLabel);

        TextBackendEntry entry = this.writeId(vertexLabel.type(), id);
        entry.column(HugeKeys.NAME.string(), vertexLabel.name());
        entry.column(HugeKeys.PRIMARY_KEYS.string(),
                toJson(vertexLabel.primaryKeys().toArray()));
        entry.column(HugeKeys.INDEX_NAMES.string(),
                toJson(vertexLabel.indexNames().toArray()));
        writeProperties(vertexLabel, entry);
        return entry;
    }

    @Override
    public BackendEntry writeEdgeLabel(EdgeLabel edgeLabel) {
        Id id = IdGeneratorFactory.generator().generate(edgeLabel);

        TextBackendEntry entry = this.writeId(edgeLabel.type(), id);
        entry.column(HugeKeys.NAME.string(), edgeLabel.name());
        entry.column(HugeKeys.FREQUENCY.string(),
                toJson(edgeLabel.frequency()));
        entry.column(HugeKeys.MULTIPLICITY.string(),
                toJson(edgeLabel.multiplicity()));
        entry.column(HugeKeys.LINKS.string(),
                toJson(edgeLabel.links().toArray()));
        entry.column(HugeKeys.SORT_KEYS.string(),
                toJson(edgeLabel.sortKeys().toArray()));
        entry.column(HugeKeys.INDEX_NAMES.string(),
                toJson(edgeLabel.indexNames().toArray()));
        writeProperties(edgeLabel, entry);
        return entry;
    }

    @Override
    public BackendEntry writePropertyKey(PropertyKey propertyKey) {
        Id id = IdGeneratorFactory.generator().generate(propertyKey);

        TextBackendEntry entry = this.writeId(propertyKey.type(), id);
        entry.column(HugeKeys.NAME.string(), propertyKey.name());
        entry.column(HugeKeys.DATA_TYPE.string(),
                toJson(propertyKey.dataType()));
        entry.column(HugeKeys.CARDINALITY.string(),
                toJson(propertyKey.cardinality()));
        writeProperties(propertyKey, entry);
        return entry;
    }

    public void writeProperties(SchemaElement schemaElement, TextBackendEntry entry) {
        Map<String, PropertyKey> properties = schemaElement.properties();
        if (properties == null) {
            entry.column(HugeKeys.PROPERTIES.string(), "[]");
        } else {
            entry.column(HugeKeys.PROPERTIES.string(),
                    toJson(properties.keySet().toArray()));
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
        String indexNames = textEntry.column(HugeKeys.INDEX_NAMES.string());

        HugeVertexLabel vertexLabel = new HugeVertexLabel(name,
                this.graph.schemaTransaction());
        vertexLabel.properties(fromJson(properties, String[].class));
        vertexLabel.primaryKeys(fromJson(primarykeys, String[].class));
        vertexLabel.indexNames(fromJson(indexNames, String[].class));

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
        String frequency = textEntry.column(HugeKeys.FREQUENCY.string());
        String sortKeys = textEntry.column(HugeKeys.SORT_KEYS.string());
        String links = textEntry.column(HugeKeys.LINKS.string());
        String properties = textEntry.column(HugeKeys.PROPERTIES.string());
        String indexNames = textEntry.column(HugeKeys.INDEX_NAMES.string());

        HugeEdgeLabel edgeLabel = new HugeEdgeLabel(name,
                this.graph.schemaTransaction());
        edgeLabel.frequency(fromJson(frequency, Frequency.class));
        edgeLabel.properties(fromJson(properties, String[].class));
        edgeLabel.sortKeys(fromJson(sortKeys, String[].class));
        edgeLabel.indexNames(fromJson(indexNames, String[].class));
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

        HugePropertyKey propertyKey = new HugePropertyKey(name,
                this.graph.schemaTransaction());
        propertyKey.dataType(fromJson(dataType, DataType.class));
        propertyKey.cardinality(fromJson(cardinality, Cardinality.class));
        propertyKey.properties(fromJson(properties, String[].class));

        return propertyKey;
    }

    @Override
    public BackendEntry writeIndexLabel(IndexLabel indexLabel) {
        Id id = IdGeneratorFactory.generator().generate(indexLabel);
        TextBackendEntry entry = this.writeId(indexLabel.type(), id);

        entry.column(HugeKeys.BASE_TYPE.string(), toJson(indexLabel.baseType()));
        entry.column(HugeKeys.BASE_VALUE.string(), toJson(indexLabel.baseValue()));
        entry.column(HugeKeys.NAME.string(), indexLabel.name());
        entry.column(HugeKeys.INDEX_TYPE.string(), toJson(indexLabel.indexType()));
        entry.column(HugeKeys.FIELDS.string(), toJson(indexLabel.indexFields().toArray()));
        return entry;
    }

    @Override
    public IndexLabel readIndexLabel(BackendEntry entry) {

        if (entry == null) {
            return null;
        }

        entry = convertEntry(entry);
        assert entry instanceof CassandraBackendEntry;

        TextBackendEntry textEntry = (TextBackendEntry) entry;
        HugeTypes baseType = fromJson(textEntry.column(HugeKeys.BASE_TYPE.string()), HugeTypes.class);
        String baseValue = textEntry.column(HugeKeys.BASE_VALUE.string());
        String indexName = textEntry.column(HugeKeys.NAME.string());
        String indexType = textEntry.column(HugeKeys.INDEX_TYPE.string());
        String indexFields = textEntry.column(HugeKeys.FIELDS.string());

        HugeIndexLabel indexLabel = new HugeIndexLabel(indexName, baseType, baseValue,
                this.graph.schemaTransaction());
        indexLabel.indexType(fromJson(indexType, IndexType.class));
        indexLabel.by(fromJson(indexFields, String[].class));

        return indexLabel;
    }

    @Override
    public BackendEntry writeIndex(HugeIndex index) {

        Id id = IdGeneratorFactory.generator().generate(index.id());
        TextBackendEntry entry = new TextBackendEntry(id);
        entry.column(HugeKeys.PROPERTY_VALUES.string(), index.propertyValues());
        entry.column(HugeKeys.INDEX_LABEL_NAME.string(), index.indexLabelName());
        entry.column(HugeKeys.ELEMENT_IDS.string(), toJson(index.elementIds().toArray()));
        return entry;
    }

    @Override
    public HugeIndex readIndex(BackendEntry backendEntry) {
        if (backendEntry == null) {
            return null;
        }

        backendEntry = convertEntry(backendEntry);
        assert backendEntry instanceof TextBackendEntry;
        TextBackendEntry entry = (TextBackendEntry) backendEntry;

        String indexValues = entry.column(HugeKeys.PROPERTY_VALUES.string());
        String indexLabelName = entry.column(HugeKeys.INDEX_LABEL_NAME.string());
        String elementIds = entry.column(HugeKeys.ELEMENT_IDS.string());

        IndexLabel indexLabel = this.graph.schemaTransaction().getIndexLabel(indexLabelName);

        HugeIndex index = new HugeIndex(indexLabel);
        index.propertyValues(indexValues);
        index.elementIds(fromJson(elementIds, Id[].class));

        return index;
    }
}
