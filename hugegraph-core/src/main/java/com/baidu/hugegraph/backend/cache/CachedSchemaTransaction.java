package com.baidu.hugegraph.backend.cache;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.schema.EdgeLabel;
import com.baidu.hugegraph.type.schema.IndexLabel;
import com.baidu.hugegraph.type.schema.PropertyKey;
import com.baidu.hugegraph.type.schema.VertexLabel;

public class CachedSchemaTransaction extends SchemaTransaction {

    private Cache cache = null;

    public CachedSchemaTransaction(HugeGraph graph, BackendStore store) {
        super(graph, store);
        this.cache = new RamCache();
    }

    public void cache(Cache cache) {
        this.cache = cache;
    }

    private Id generateId(HugeType type, String name) {
        // NOTE: it's slower performance to use:
        // String.format("%x-%s", type.code(), name)
        return this.idGenerator.generate(type.code() + "-" + name);
    }

    @Override
    public VertexLabel getVertexLabel(String name) {
        Id id = generateId(HugeType.VERTEX_LABEL, name);
        Object value = this.cache.getOrFetch(id, k -> {
            return super.getVertexLabel(name);
        });
        return (VertexLabel) value;
    }

    @Override
    public EdgeLabel getEdgeLabel(String name) {
        Id id = generateId(HugeType.EDGE_LABEL, name);
        Object value = this.cache.getOrFetch(id, k -> {
            return super.getEdgeLabel(name);
        });
        return (EdgeLabel) value;
    }

    @Override
    public PropertyKey getPropertyKey(String name) {
        Id id = generateId(HugeType.PROPERTY_KEY, name);
        Object value = this.cache.getOrFetch(id, k -> {
            return super.getPropertyKey(name);
        });
        return (PropertyKey) value;
    }

    @Override
    public IndexLabel getIndexLabel(String name) {
        Id id = generateId(HugeType.INDEX_LABEL, name);
        Object value = this.cache.getOrFetch(id, k -> {
            return super.getIndexLabel(name);
        });
        return (IndexLabel) value;
    }

    @Override
    protected void addSchema(SchemaElement e, BackendEntry entry) {
        Id id = this.generateId(e.type(), e.name());
        this.cache.invalidate(id);

        super.addSchema(e, entry);
    }

    @Override
    protected void removeSchema(SchemaElement e) {
        Id id = this.generateId(e.type(), e.name());
        this.cache.invalidate(id);

        super.removeSchema(e);
    }
}
