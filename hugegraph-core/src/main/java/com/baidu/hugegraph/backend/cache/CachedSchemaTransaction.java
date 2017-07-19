package com.baidu.hugegraph.backend.cache;

import java.util.List;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.event.EventHub;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.schema.EdgeLabel;
import com.baidu.hugegraph.type.schema.IndexLabel;
import com.baidu.hugegraph.type.schema.PropertyKey;
import com.baidu.hugegraph.type.schema.VertexLabel;
import com.baidu.hugegraph.util.Events;
import com.google.common.collect.ImmutableList;

public class CachedSchemaTransaction extends SchemaTransaction {

    private final Cache cache;

    public CachedSchemaTransaction(HugeGraph graph, BackendStore store) {
        super(graph, store);
        this.cache = CacheManager.instance().cache("schema-" + graph.name());

        this.listenChanges();
    }

    private void listenChanges() {
        // Listen store event: "store.init", "store.clear"
        List<String> events = ImmutableList.of(Events.STORE_INIT,
                                               Events.STORE_CLEAR);
        super.store().provider().listen(event -> {
            if (events.contains(event.name())) {
                logger.info("Clear cache on event '{}'", event.name());
                this.cache.clear();
                return true;
            }
            return false;
        });

        // Listen cache event: "cache"(invalid cache item)
        EventHub schemaEventHub = super.graph().schemaEventHub();
        if (!schemaEventHub.containsListener(Events.CACHE)) {
            schemaEventHub.listen(Events.CACHE, event -> {
                logger.debug("Received event: {}", event);
                event.checkArgs(String.class, Id.class);
                Object[] args = event.args();
                if (args[0].equals("invalid")) {
                    Id id = (Id) args[1];
                    this.cache.invalidate(id);
                    return true;
                }
                return false;
            });
        }
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
