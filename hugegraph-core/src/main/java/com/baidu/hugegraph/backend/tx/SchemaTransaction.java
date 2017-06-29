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

    public List<PropertyKey> getPropertyKeys() {
        List<PropertyKey> propertyKeys = new ArrayList<>();
        Query q = new Query(HugeType.PROPERTY_KEY);
        Iterable<BackendEntry> entries = query(q);
        entries.forEach(i -> {
            propertyKeys.add(this.serializer.readPropertyKey(i));
        });
        return propertyKeys;
    }

    public List<VertexLabel> getVertexLabels() {
        List<VertexLabel> vertexLabels = new ArrayList<>();
        Query q = new Query(HugeType.VERTEX_LABEL);
        Iterable<BackendEntry> entries = query(q);
        entries.forEach(i -> {
            vertexLabels.add(this.serializer.readVertexLabel(i));
        });
        return vertexLabels;
    }

    public List<EdgeLabel> getEdgeLabels() {
        List<EdgeLabel> edgeLabels = new ArrayList<>();
        Query q = new Query(HugeType.EDGE_LABEL);
        Iterable<BackendEntry> entries = query(q);
        entries.forEach(i -> {
            edgeLabels.add(this.serializer.readEdgeLabel(i));
        });
        return edgeLabels;
    }

    public List<IndexLabel> getIndexLabels() {
        List<IndexLabel> indexLabels = new ArrayList<>();
        Query q = new Query(HugeType.INDEX_LABEL);
        Iterable<BackendEntry> entries = query(q);
        entries.forEach(i -> {
            indexLabels.add(this.serializer.readIndexLabel(i));
        });
        return indexLabels;
    }

    public void addPropertyKey(PropertyKey propKey) {
        logger.debug("SchemaTransaction add property key: {}", propKey);
        this.addSchema(propKey, this.serializer.writePropertyKey(propKey));
    }

    public PropertyKey getPropertyKey(String name) {
        BackendEntry entry = querySchema(new HugePropertyKey(name));
        return this.serializer.readPropertyKey(entry);
    }

    public void removePropertyKey(String name) {
        logger.debug("SchemaTransaction remove property key '{}'", name);
        this.removeSchema(new HugePropertyKey(name));
    }

    public void addVertexLabel(VertexLabel vertexLabel) {
        logger.debug("SchemaTransaction add vertex label: {}", vertexLabel);
        BackendEntry entry = this.serializer.writeVertexLabel(vertexLabel);
        this.addSchema(vertexLabel, entry);
    }

    public VertexLabel getVertexLabel(String name) {
        BackendEntry entry = querySchema(new HugeVertexLabel(name));
        return this.serializer.readVertexLabel(entry);
    }

    public void removeVertexLabel(String name) {
        logger.debug("SchemaTransaction remove vertex label '{}'", name);
        this.removeSchema(new HugeVertexLabel(name));
    }

    public void addEdgeLabel(EdgeLabel edgeLabel) {
        logger.debug("SchemaTransaction add edge label: {}", edgeLabel);
        this.addSchema(edgeLabel, this.serializer.writeEdgeLabel(edgeLabel));
    }

    public EdgeLabel getEdgeLabel(String name) {
        BackendEntry entry = querySchema(new HugeEdgeLabel(name));
        return this.serializer.readEdgeLabel(entry);
    }

    public void removeEdgeLabel(String name) {
        logger.debug("SchemaTransaction remove edge label '{}'", name);
        this.removeSchema(new HugeEdgeLabel(name));
    }

    public void addIndexLabel(IndexLabel indexLabel) {
        logger.debug("SchemaTransaction add index label: {}", indexLabel);
        this.addSchema(indexLabel, this.serializer.writeIndexLabel(indexLabel));
    }

    public IndexLabel getIndexLabel(String name) {
        BackendEntry entry = querySchema(new HugeIndexLabel(name));
        return this.serializer.readIndexLabel(entry);
    }

    public void removeIndexLabel(String name) {
        logger.debug("SchemaTransaction remove index label '{}'", name);
        // TODO: need check index data exists
        this.removeSchema(new HugeIndexLabel(name));
    }

    protected void addSchema(SchemaElement schemaElement, BackendEntry entry) {
        this.beforeWrite();
        this.addEntry(entry);
        this.afterWrite();
    }

    protected BackendEntry querySchema(SchemaElement schemaElement) {
        Id id = this.idGenerator.generate(schemaElement);
        this.beforeRead();
        BackendEntry entry = this.query(schemaElement.type(), id);
        this.afterRead();
        return entry;
    }

    protected void removeSchema(SchemaElement schemaElement) {
        Id id = this.idGenerator.generate(schemaElement);
        this.beforeWrite();
        this.removeEntry(schemaElement.type(), id);
        this.afterWrite();
    }
}
