package com.baidu.hugegraph.backend.tx;

import java.util.ArrayList;
import java.util.List;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.schema.HugeEdgeLabel;
import com.baidu.hugegraph.schema.HugeIndexLabel;
import com.baidu.hugegraph.schema.HugePropertyKey;
import com.baidu.hugegraph.schema.HugeVertexLabel;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.schema.EdgeLabel;
import com.baidu.hugegraph.type.schema.IndexLabel;
import com.baidu.hugegraph.type.schema.PropertyKey;
import com.baidu.hugegraph.type.schema.VertexLabel;

public class SchemaTransaction extends AbstractTransaction {

    public SchemaTransaction(HugeGraph graph, BackendStore store) {
        super(graph, store);
    }

    public List<HugePropertyKey> getPropertyKeys() {
        List<HugePropertyKey> propertyKeys = new ArrayList<HugePropertyKey>();
        Query q = new Query(HugeType.PROPERTY_KEY);
        Iterable<BackendEntry> entries = query(q);
        entries.forEach(item -> {
            propertyKeys.add((HugePropertyKey) this.serializer.readPropertyKey(item));
        });
        return propertyKeys;
    }

    public List<HugeVertexLabel> getVertexLabels() {
        List<HugeVertexLabel> vertexLabels = new ArrayList<>();
        Query q = new Query(HugeType.VERTEX_LABEL);
        Iterable<BackendEntry> entries = query(q);
        entries.forEach(item -> {
            vertexLabels.add((HugeVertexLabel) this.serializer.readVertexLabel(item));
        });
        return vertexLabels;
    }

    public List<HugeEdgeLabel> getEdgeLabels() {
        List<HugeEdgeLabel> edgeLabels = new ArrayList<>();
        Query q = new Query(HugeType.EDGE_LABEL);
        Iterable<BackendEntry> entries = query(q);
        entries.forEach(item -> {
            edgeLabels.add((HugeEdgeLabel) this.serializer.readEdgeLabel(item));
        });
        return edgeLabels;
    }

    public List<HugeIndexLabel> getIndexLabels() {
        List<HugeIndexLabel> indexLabels = new ArrayList<>();
        Query q = new Query(HugeType.INDEX_LABEL);
        Iterable<BackendEntry> entries = query(q);
        entries.forEach(item -> {
            indexLabels.add((HugeIndexLabel) this.serializer.readIndexLabel(item));
        });
        return indexLabels;
    }

    public void addPropertyKey(PropertyKey propertyKey) {
        logger.debug("SchemaTransaction add property key, "
                + "name: " + propertyKey.name() + ", "
                + "dataType: " + propertyKey.dataType() + ", "
                + "cardinality: " + propertyKey.cardinality());

        this.addSchema(this.serializer.writePropertyKey(propertyKey));
    }

    public PropertyKey getPropertyKey(String name) {
        BackendEntry entry = querySchema(new HugePropertyKey(name));
        return this.serializer.readPropertyKey(entry);
    }

    public void removePropertyKey(String name) {
        logger.debug("SchemaTransaction remove property key " + name);

        this.removeSchema(new HugePropertyKey(name));
    }

    public void addVertexLabel(VertexLabel vertexLabel) {
        logger.debug("SchemaTransaction add vertex label, "
                + "name: " + vertexLabel.name());

        this.addSchema(this.serializer.writeVertexLabel(vertexLabel));
    }

    public VertexLabel getVertexLabel(String name) {
        BackendEntry entry = querySchema(new HugeVertexLabel(name));
        return this.serializer.readVertexLabel(entry);
    }

    public void removeVertexLabel(String name) {
        logger.info("SchemaTransaction remove vertex label " + name);

        this.removeSchema(new HugeVertexLabel(name));
    }

    public void addEdgeLabel(EdgeLabel edgeLabel) {
        logger.debug("SchemaTransaction add edge label, "
                + "name: " + edgeLabel.name() + ", "
                + "frequency: " + edgeLabel.frequency());

        this.addSchema(this.serializer.writeEdgeLabel(edgeLabel));
    }

    public EdgeLabel getEdgeLabel(String name) {
        BackendEntry entry = querySchema(new HugeEdgeLabel(name));
        return this.serializer.readEdgeLabel(entry);
    }

    public void removeEdgeLabel(String name) {
        logger.info("SchemaTransaction remove edge label " + name);

        this.removeSchema(new HugeEdgeLabel(name));
    }

    public void addIndexLabel(IndexLabel indexLabel) {
        logger.debug("SchemaTransaction add index label, "
                + "name: " + indexLabel.name() + ", "
                + "base-type: " + indexLabel.baseType() + ", "
                + "base-value:" + indexLabel.baseValue() + ", "
                + "indexType: " + indexLabel.indexType() + ", "
                + "fields: " + indexLabel.indexFields());

        this.addSchema(this.serializer.writeIndexLabel(indexLabel));
    }

    public IndexLabel getIndexLabel(String name) {
        BackendEntry entry = querySchema(new HugeIndexLabel(name));
        return this.serializer.readIndexLabel(entry);
    }

    public void removeIndexLabel(String name) {
        logger.info("SchemaTransaction remove index label " + name);
        // TODO: need check index data exists
        this.removeSchema(new HugeIndexLabel(name));
    }

    private void addSchema(BackendEntry entry) {
        this.beforeWrite();
        this.addEntry(entry);
        this.afterWrite();
    }

    private BackendEntry querySchema(SchemaElement schemaElement) {
        Id id = this.idGenerator.generate(schemaElement);
        this.beforeRead();
        BackendEntry entry = this.query(schemaElement.type(), id);
        this.afterRead();
        return entry;
    }

    private void removeSchema(SchemaElement schemaElement) {
        Id id = this.idGenerator.generate(schemaElement);
        this.beforeWrite();
        this.removeEntry(schemaElement.type(), id);
        this.afterWrite();
    }
}
