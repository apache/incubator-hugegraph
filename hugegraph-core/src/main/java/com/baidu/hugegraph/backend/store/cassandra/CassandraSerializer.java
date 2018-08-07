package com.baidu.hugegraph.backend.store.cassandra;

import java.util.Collection;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Direction;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGeneratorFactory;
import com.baidu.hugegraph.backend.serializer.AbstractSerializer;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.cassandra.CassandraBackendEntry.Cell;
import com.baidu.hugegraph.backend.store.cassandra.CassandraBackendEntry.Row;
import com.baidu.hugegraph.schema.HugeEdgeLabel;
import com.baidu.hugegraph.schema.HugePropertyKey;
import com.baidu.hugegraph.schema.HugeVertexLabel;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeProperty;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.HugeTypes;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.define.DataType;
import com.baidu.hugegraph.type.define.Frequency;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.type.schema.EdgeLabel;
import com.baidu.hugegraph.type.schema.PropertyKey;
import com.baidu.hugegraph.type.schema.VertexLabel;
import com.google.gson.Gson;

public class CassandraSerializer extends AbstractSerializer {

    private static Gson gson = new Gson();

    public CassandraSerializer(final HugeGraph graph) {
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
        return new CassandraBackendEntry(id);
    }

    protected CassandraBackendEntry newBackendEntry(HugeElement e) {
        return new CassandraBackendEntry(e.type(), e.id());
    }

    protected CassandraBackendEntry newBackendEntry(SchemaElement e) {
        Id id = IdGeneratorFactory.generator().generate(e);
        return new CassandraBackendEntry(e.type(), id);
    }

    @Override
    protected BackendEntry convertEntry(BackendEntry entry) {
        if (entry instanceof CassandraBackendEntry) {
            return entry;
        }
        else {
            throw new BackendException(
                    "CassandraSerializer just supports CassandraBackendEntry");
        }
    }

    protected Cell formatProperty(HugeProperty<?> prop) {
        return new Cell(HugeKeys.PROPERTY_KEY, prop.key(),
                HugeKeys.PROPERTY_VALUE, toJson(prop.value()));
    }

    protected void parseProperty(String colName, String colValue, HugeElement owner) {
        // get PropertyKey by PropertyKey name
        PropertyKey pkey = this.graph.openSchemaManager().propertyKey(colName);

        // parse value
        Object value = fromJson(colValue, pkey.clazz());

        // set properties of vertex/edge
        if (pkey.cardinality() == Cardinality.SINGLE) {
            owner.property(pkey.name(), value);
        }
        else {
            if (value instanceof Collection) {
                for (Object v : (Collection<?>) value) {
                    owner.property(pkey.name(), v);
                }
            }
            else {
                assert false : "invalid value of non-sigle property";
            }
        }
    }

    protected Row formatEdge(HugeEdge edge) {
        Row row = new Row(HugeTypes.EDGE, edge.sourceVertex().id());

        // sourceVertex + direction + edge-label-name + sortValues + targetVertex
        row.key(HugeKeys.SOURCE_VERTEX, edge.owner().id().asString());
        row.key(HugeKeys.DIRECTION, edge.direction().ordinal());
        row.key(HugeKeys.LABEL, edge.label());
        row.key(HugeKeys.SORT_VALUES, edge.name());
        row.key(HugeKeys.TARGET_VERTEX, edge.otherVertex().id().asString());

        // edge properties
        for (HugeProperty<?> property : edge.getProperties().values()) {
            row.cell(this.formatProperty(property));
        }

        // TODO: fill a default property if non, it should be improved!
        if (edge.getProperties().isEmpty()) {
            row.cell(new Cell(HugeKeys.PROPERTY_KEY, "$",
                    HugeKeys.PROPERTY_VALUE, "1"));
        }

        return row;
    }

    // parse an edge from a sub row
    protected void parseEdge(Row row, HugeVertex vertex) {
        String sourceVertexId = row.key(HugeKeys.SOURCE_VERTEX);
        int direction = Integer.parseInt(row.key(HugeKeys.DIRECTION));
        String labelName = row.key(HugeKeys.LABEL);
        String targetVertexId = row.key(HugeKeys.TARGET_VERTEX);

        boolean isOutEdge = (direction == Direction.OUT.ordinal());
        EdgeLabel label = this.graph.openSchemaManager().edgeLabel(labelName);

        // TODO: how to construct targetVertex with id
        Id otherVertexId = IdGeneratorFactory.generator().generate(targetVertexId);
        HugeVertex otherVertex = new HugeVertex(this.graph, otherVertexId, null);

        Id id = IdGeneratorFactory.generator().generate(sourceVertexId);

        HugeEdge edge = new HugeEdge(this.graph, id, label);

        if (isOutEdge) {
            edge.targetVertex(otherVertex);
            vertex.addOutEdge(edge);
        } else {
            edge.sourceVertex(otherVertex);
            vertex.addInEdge(edge);
        }

        // edge properties
        for (Cell cell : row.cells()) {
            this.parseProperty(cell.name(), cell.value(), edge);
        }
    }

    @Override
    public BackendEntry writeVertex(HugeVertex vertex) {
        CassandraBackendEntry entry = newBackendEntry(vertex);

        // id with label
        entry.column(HugeKeys.ID, entry.id().asString()); // add id to cells
        // entry.column(HugeKeys.LABEL, vertex.label());

        // add all properties of a Vertex
        for (HugeProperty<?> prop : vertex.getProperties().values()) {
            entry.column(this.formatProperty(prop));
        }

        // add all edges of a Vertex
        for (HugeEdge edge : vertex.getEdges()) {
            entry.subRow(this.formatEdge(edge));
        }

        // test readVertex
//        System.out.println("writeVertex:" + entry);
//        HugeVertex v = readVertex(entry);
//        System.out.println("readVertex:" + v);

        return entry;
    }

    @Override
    public HugeVertex readVertex(BackendEntry bytesEntry) {
        if (bytesEntry == null) {
            return null;
        }
        bytesEntry = this.convertEntry(bytesEntry);
        assert bytesEntry instanceof CassandraBackendEntry;
        CassandraBackendEntry entry = (CassandraBackendEntry) bytesEntry;

        // id with label
        // String labelName = entry.column(HugeKeys.LABEL);
        // TODO: improve Id split()
        String labelName = IdGeneratorFactory.generator().split(
                entry.id())[0].asString();
        VertexLabel label = this.graph.openSchemaManager().vertexLabel(labelName);

        // id
        HugeVertex vertex = new HugeVertex(this.graph, entry.id(), label);

        // parse all properties of a Vertex
        for (Cell cell : entry.cells()) {
            this.parseProperty(cell.name(), cell.value(), vertex);
        }

        // parse all edges of a Vertex
        for (Row edge : entry.subRows()) {
            this.parseEdge(edge, vertex);
        }
        return vertex;
    }

    @Override
    public BackendEntry writeId(Id id) {
        return newBackendEntry(id);
    }

    @Override
    public BackendEntry writeVertexLabel(VertexLabel vertexLabel) {
        CassandraBackendEntry entry = newBackendEntry(vertexLabel);
        entry.column(HugeKeys.NAME, vertexLabel.name());
        entry.column(HugeKeys.PRIMARY_KEYS,
                toJson(vertexLabel.primaryKeys().toArray()));
        writeProperties(vertexLabel, entry);
        return entry;
    }

    @Override
    public BackendEntry writeEdgeLabel(EdgeLabel edgeLabel) {
        CassandraBackendEntry entry = newBackendEntry(edgeLabel);
        entry.column(HugeKeys.NAME, edgeLabel.name());
        entry.column(HugeKeys.FREQUENCY, toJson(edgeLabel.frequency()));
        entry.column(HugeKeys.MULTIPLICITY, toJson(edgeLabel.multiplicity()));
        // entry.column(HugeKeys.LINKS, toJson(edgeLabel.links().toArray()));
        entry.column(HugeKeys.SORT_KEYS, toJson(edgeLabel.sortKeys().toArray()));
        writeProperties(edgeLabel, entry);
        return entry;
    }

    @Override
    public BackendEntry writePropertyKey(PropertyKey propertyKey) {
        CassandraBackendEntry entry = newBackendEntry(propertyKey);
        entry.column(HugeKeys.NAME, propertyKey.name());
        entry.column(HugeKeys.DATA_TYPE, toJson(propertyKey.dataType()));
        entry.column(HugeKeys.CARDINALITY, toJson(propertyKey.cardinality()));
        writeProperties(propertyKey, entry);
        return entry;
    }

    public void writeProperties(SchemaElement schemaElement, CassandraBackendEntry entry) {
        Map<String, PropertyKey> properties = schemaElement.properties();
        if (properties == null) {
            entry.column(HugeKeys.PROPERTIES, "[]");
        } else {
            entry.column(HugeKeys.PROPERTIES,
                    toJson(properties.keySet().toArray()));
        }
    }

    @Override
    public VertexLabel readVertexLabel(BackendEntry entry) {
        if (entry == null) {
            return null;
        }

        entry = convertEntry(entry);
        assert entry instanceof CassandraBackendEntry;

        CassandraBackendEntry textEntry = (CassandraBackendEntry) entry;
        String name = textEntry.column(HugeKeys.NAME);
        String properties = textEntry.column(HugeKeys.PROPERTIES);
        String primarykeys = textEntry.column(HugeKeys.PRIMARY_KEYS);

        HugeVertexLabel vertexLabel = new HugeVertexLabel(name,
                this.graph.openSchemaTransaction());
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
        assert entry instanceof CassandraBackendEntry;

        CassandraBackendEntry textEntry = (CassandraBackendEntry) entry;
        String name = textEntry.column(HugeKeys.NAME);
        String frequency = textEntry.column(HugeKeys.FREQUENCY);
        String sortKeys = textEntry.column(HugeKeys.SORT_KEYS);
        String links = textEntry.column(HugeKeys.LINKS);
        String properties = textEntry.column(HugeKeys.PROPERTIES);

        HugeEdgeLabel edgeLabel = new HugeEdgeLabel(name,
                this.graph.openSchemaTransaction());
        edgeLabel.frequency(fromJson(frequency, Frequency.class));
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
        assert entry instanceof CassandraBackendEntry;

        CassandraBackendEntry textEntry = (CassandraBackendEntry) entry;
        String name = textEntry.column(HugeKeys.NAME);
        String dataType = textEntry.column(HugeKeys.DATA_TYPE);
        String cardinality = textEntry.column(HugeKeys.CARDINALITY);
        String properties = textEntry.column(HugeKeys.PROPERTIES);

        HugePropertyKey propertyKey = new HugePropertyKey(name,
                this.graph.openSchemaTransaction());
        propertyKey.dataType(fromJson(dataType, DataType.class));
        propertyKey.cardinality(fromJson(cardinality, Cardinality.class));
        propertyKey.properties(fromJson(properties, String[].class));

        return propertyKey;
    }
}
