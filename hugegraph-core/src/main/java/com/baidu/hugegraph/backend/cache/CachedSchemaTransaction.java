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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.event.EventHub;
import com.baidu.hugegraph.event.EventListener;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.Events;
import com.google.common.collect.ImmutableSet;

public final class CachedSchemaTransaction extends SchemaTransaction {

    private final Cache<Id, Object> idCache;
    private final Cache<Id, Object> nameCache;

    private EventListener storeEventListener;
    private EventListener cacheEventListener;

    private static final Map<String, CachedTypes> CACHED_TYPES =
                                                  new ConcurrentHashMap<>();

    public CachedSchemaTransaction(HugeGraphParams graph, BackendStore store) {
        super(graph, store);

        this.idCache = this.cache("schema-id");
        this.nameCache = this.cache("schema-name");

        this.listenChanges();
    }

    @Override
    public void close() {
        try {
            super.close();
        } finally {
            this.clearCache();
            this.unlistenChanges();
        }
    }

    private Cache<Id, Object> cache(String prefix) {
        HugeConfig conf = super.params().configuration();

        final String name = prefix + "-" + this.graphName();
        final long capacity = conf.get(CoreOptions.SCHEMA_CACHE_CAPACITY);
        // NOTE: must disable schema cache-expire due to getAllSchema()
        return CacheManager.instance().cache(name, capacity);
    }

    private CachedTypes cachedTypes() {
        String graph = this.params().name();
        if (!CACHED_TYPES.containsKey(graph)) {
            CACHED_TYPES.putIfAbsent(graph, new CachedTypes());
        }
        return CACHED_TYPES.get(graph);
    }

    private void listenChanges() {
        // Listen store event: "store.init", "store.clear", ...
        Set<String> storeEvents = ImmutableSet.of(Events.STORE_INIT,
                                                  Events.STORE_CLEAR,
                                                  Events.STORE_TRUNCATE);
        this.storeEventListener = event -> {
            if (storeEvents.contains(event.name())) {
                LOG.debug("Graph {} clear schema cache on event '{}'",
                          this.graph(), event.name());
                this.clearCache();
                return true;
            }
            return false;
        };
        this.store().provider().listen(this.storeEventListener);

        // Listen cache event: "cache"(invalid cache item)
        this.cacheEventListener = event -> {
            LOG.debug("Graph {} received schema cache event: {}",
                      this.graph(), event);
            event.checkArgs(String.class, Id.class);
            Object[] args = event.args();
            if ("invalid".equals(args[0])) {
                Id id = (Id) args[1];
                Object value = this.idCache.get(id);
                if (value != null) {
                    // Invalidate id cache
                    this.idCache.invalidate(id);

                    // Invalidate name cache
                    SchemaElement schema = (SchemaElement) value;
                    Id prefixedName = generateId(schema.type(),
                                                 schema.name());
                    this.nameCache.invalidate(prefixedName);
                }
                return true;
            } else if ("clear".equals(args[0])) {
                this.clearCache();
                return true;
            }
            return false;
        };
        EventHub schemaEventHub = this.params().schemaEventHub();
        if (!schemaEventHub.containsListener(Events.CACHE)) {
            schemaEventHub.listen(Events.CACHE, this.cacheEventListener);
        }
    }

    private void clearCache() {
        this.idCache.clear();
        this.nameCache.clear();
        this.cachedTypes().clear();
    }

    private void unlistenChanges() {
        // Unlisten store event
        this.store().provider().unlisten(this.storeEventListener);

        // Unlisten cache event
        EventHub schemaEventHub = this.params().schemaEventHub();
        schemaEventHub.unlisten(Events.CACHE, this.cacheEventListener);
    }

    private void resetCachedAllIfReachedCapacity() {
        if (this.idCache.size() >= this.idCache.capacity()) {
            LOG.warn("Schema cache reached capacity({}): {}",
                     this.idCache.capacity(), this.idCache.size());
            this.cachedTypes().clear();
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

    @Override
    protected void addSchema(SchemaElement schema) {
        super.addSchema(schema);

        this.resetCachedAllIfReachedCapacity();

        Id prefixedId = generateId(schema.type(), schema.id());
        this.idCache.update(prefixedId, schema);

        Id prefixedName = generateId(schema.type(), schema.name());
        this.nameCache.update(prefixedName, schema);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <T extends SchemaElement> T getSchema(HugeType type, Id id) {
        Id prefixedId = generateId(type, id);
        Object value = this.idCache.get(prefixedId);
        if (value == null) {
            value = super.getSchema(type, id);
            if (value != null) {
                this.resetCachedAllIfReachedCapacity();

                this.idCache.update(prefixedId, value);

                SchemaElement schema = (SchemaElement) value;
                Id prefixedName = generateId(schema.type(), schema.name());
                this.nameCache.update(prefixedName, schema);
            }
        }
        return (T) value;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <T extends SchemaElement> T getSchema(HugeType type,
                                                    String name) {
        Id prefixedName = generateId(type, name);
        Object value = this.nameCache.get(prefixedName);
        if (value == null) {
            value = super.getSchema(type, name);
            if (value != null) {
                this.resetCachedAllIfReachedCapacity();

                this.nameCache.update(prefixedName, value);

                SchemaElement schema = (SchemaElement) value;
                Id prefixedId = generateId(schema.type(), schema.id());
                this.idCache.update(prefixedId, schema);
            }
        }
        return (T) value;
    }

    @Override
    protected void removeSchema(SchemaElement schema) {
        super.removeSchema(schema);

        Id prefixedId = generateId(schema.type(), schema.id());
        Object value = this.idCache.get(prefixedId);
        if (value != null) {
            this.idCache.invalidate(prefixedId);

            schema = (SchemaElement) value;
            Id prefixedName = generateId(schema.type(), schema.name());
            this.nameCache.invalidate(prefixedName);
        }
    }

    @Override
    protected <T extends SchemaElement> List<T> getAllSchema(HugeType type) {
        Boolean cachedAll = this.cachedTypes().getOrDefault(type, false);
        if (cachedAll) {
            List<T> results = new ArrayList<>();
            // Get from cache
            this.idCache.traverse(value -> {
                @SuppressWarnings("unchecked")
                T schema = (T) value;
                if (schema.type() == type) {
                    results.add(schema);
                }
            });
            return results;
        } else {
            List<T> results = super.getAllSchema(type);
            long free = this.idCache.capacity() - this.idCache.size();
            if (results.size() <= free) {
                // Update cache
                for (T schema : results) {
                    Id prefixedId = generateId(schema.type(), schema.id());
                    this.idCache.update(prefixedId, schema);

                    Id prefixedName = generateId(schema.type(), schema.name());
                    this.nameCache.update(prefixedName, schema);
                }
                this.cachedTypes().putIfAbsent(type, true);
            }
            return results;
        }
    }

    private static class CachedTypes
                   extends ConcurrentHashMap<HugeType, Boolean> {

        private static final long serialVersionUID = -2215549791679355996L;
    }
}
