/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.backend.cache;

import java.util.List;
import java.util.function.Function;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.event.EventHub;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.Events;
import com.google.common.collect.ImmutableList;

public class CachedSchemaTransaction extends SchemaTransaction {

    private final Cache idCache;
    private final Cache nameCache;

    public CachedSchemaTransaction(HugeGraph graph, BackendStore store) {
        super(graph, store);
        this.idCache = this.cache("schema-id");
        this.nameCache = this.cache("schema-name");

        this.listenChanges();
    }

    private Cache cache(String prefix) {
        HugeConfig conf = super.graph().configuration();

        final String name = prefix + "-" + super.graph().name();
        final int capacity = conf.get(CoreOptions.SCHEMA_CACHE_CAPACITY);
        final int expire = conf.get(CoreOptions.SCHEMA_CACHE_EXPIRE);

        Cache cache = CacheManager.instance().cache(name, capacity);
        cache.expire(expire);
        return cache;
    }

    private void listenChanges() {
        // Listen store event: "store.init", "store.clear"
        List<String> events = ImmutableList.of(Events.STORE_INIT,
                                               Events.STORE_CLEAR);
        super.store().provider().listen(event -> {
            if (events.contains(event.name())) {
                LOG.info("Clear cache on event '{}'", event.name());
                this.idCache.clear();
                this.nameCache.clear();
                return true;
            }
            return false;
        });

        // Listen cache event: "cache"(invalid cache item)
        EventHub schemaEventHub = super.graph().schemaEventHub();
        if (!schemaEventHub.containsListener(Events.CACHE)) {
            schemaEventHub.listen(Events.CACHE, event -> {
                LOG.debug("Received event: {}", event);
                event.checkArgs(String.class, Id.class);
                Object[] args = event.args();
                if (args[0].equals("invalid")) {
                    Id id = (Id) args[1];
                    Object value = this.idCache.get(id);
                    this.idCache.invalidate(id);
                    if (value != null) {
                        SchemaElement schema = (SchemaElement) value;
                        this.nameCache.invalidate(generateId(schema.type(),
                                                             schema.name()));
                    }
                    return true;
                }
                return false;
            });
        }
    }

    private static Id generateId(HugeType type, Id id) {
        // NOTE: it's slower performance to use:
        // String.format("%x-%s", type.code(), name)
        return IdGenerator.of(type.string() + "-" + id.asString());
    }

    private static Id generateId(HugeType type, String name) {
        return IdGenerator.of(type.string() + "-" + name);
    }

    private Object getOrFetch(HugeType type, Id id,
                              Function<Id, Object> fetcher) {
        Id prefixedId = generateId(type, id);
        Object value = this.idCache.get(prefixedId);
        if (value == null) {
            value = fetcher.apply(id);
            if (value != null) {
                this.idCache.update(prefixedId, value);
                SchemaElement schema = (SchemaElement) value;
                Id prefixedName = generateId(schema.type(), schema.name());
                this.nameCache.update(prefixedName, schema);
            }
        }
        return value;
    }

    private Object getOrFetch(HugeType type, String name,
                              Function<String, Object> fetcher) {
        Id prefixedName = generateId(type, name);
        Object value = this.nameCache.get(prefixedName);
        if (value == null) {
            value = fetcher.apply(name);
            if (value != null) {
                this.nameCache.update(prefixedName, value);
                SchemaElement schema = (SchemaElement) value;
                Id prefixedId = generateId(schema.type(), schema.id());
                this.idCache.update(prefixedId, schema);
            }
        }
        return value;
    }

    @Override
    public VertexLabel getVertexLabel(Id id) {
        Object value = this.getOrFetch(HugeType.VERTEX_LABEL, id,
                                       k -> super.getVertexLabel(id));
        return (VertexLabel) value;
    }

    @Override
    public VertexLabel getVertexLabel(String name) {
        Object value = this.getOrFetch(HugeType.VERTEX_LABEL, name,
                                       k -> super.getVertexLabel(name));
        return (VertexLabel) value;
    }

    @Override
    public EdgeLabel getEdgeLabel(Id id) {
        Object value = this.getOrFetch(HugeType.EDGE_LABEL, id,
                                       k -> super.getEdgeLabel(id));
        return (EdgeLabel) value;
    }

    @Override
    public EdgeLabel getEdgeLabel(String name) {
        Object value = this.getOrFetch(HugeType.EDGE_LABEL, name,
                                       k -> super.getEdgeLabel(name));
        return (EdgeLabel) value;
    }

    @Override
    public PropertyKey getPropertyKey(Id id) {
        Object value = this.getOrFetch(HugeType.PROPERTY_KEY, id,
                                       k -> super.getPropertyKey(id));
        return (PropertyKey) value;
    }

    @Override
    public PropertyKey getPropertyKey(String name) {
        Object value = this.getOrFetch(HugeType.PROPERTY_KEY, name,
                                       k -> super.getPropertyKey(name));
        return (PropertyKey) value;
    }

    @Override
    public IndexLabel getIndexLabel(Id id) {
        Object value = this.getOrFetch(HugeType.INDEX_LABEL, id,
                                       k -> super.getIndexLabel(id));
        return (IndexLabel) value;
    }

    @Override
    public IndexLabel getIndexLabel(String name) {
        Object value = this.getOrFetch(HugeType.INDEX_LABEL, name,
                                       k -> super.getIndexLabel(name));
        return (IndexLabel) value;
    }

    @Override
    protected void addSchema(SchemaElement schema) {
        super.addSchema(schema);
        Id prefixedId = generateId(schema.type(), schema.id());
        this.idCache.update(prefixedId, schema);

        Id prefixedName = generateId(schema.type(), schema.name());
        this.nameCache.update(prefixedName, schema);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <T extends SchemaElement> T getSchema(HugeType type, Id id) {
        Object value = this.getOrFetch(type, id,
                                       k -> super.getSchema(type, id));
        return (T) value;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <T extends SchemaElement> T getSchema(HugeType type,
                                                    String name) {
        Object value = this.getOrFetch(type, name,
                                       k -> super.getSchema(type, name));
        return (T) value;
    }

    @Override
    protected void removeSchema(HugeType type, Id id) {
        super.removeSchema(type, id);
        Id prefixedId = generateId(type, id);
        Object value = this.idCache.get(prefixedId);
        if (value != null) {
            this.idCache.invalidate(prefixedId);
            SchemaElement schema = (SchemaElement) value;
            this.nameCache.invalidate(generateId(type, schema.name()));
        }
    }
}
