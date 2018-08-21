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
import java.util.function.Function;

import com.baidu.hugegraph.HugeGraph;
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

public class CachedSchemaTransaction extends SchemaTransaction {

    private final Cache idCache;
    private final Cache nameCache;

    private EventListener storeEventListener;
    private EventListener cacheEventListener;

    private final Map<HugeType, Boolean> cachedTypes;

    public CachedSchemaTransaction(HugeGraph graph, BackendStore store) {
        super(graph, store);

        this.idCache = this.cache("schema-id");
        this.nameCache = this.cache("schema-name");

        this.cachedTypes = new ConcurrentHashMap<>();

        this.listenChanges();
    }

    @Override
    public void close() {
        super.close();
        this.unlistenChanges();
    }

    private Cache cache(String prefix) {
        HugeConfig conf = super.graph().configuration();

        final String name = prefix + "-" + super.graph().name();
        final int capacity = conf.get(CoreOptions.SCHEMA_CACHE_CAPACITY);
        // NOTE: must disable schema cache-expire due to getAllSchema()
        return CacheManager.instance().cache(name, capacity);
    }

    private void listenChanges() {
        // Listen store event: "store.init", "store.clear", ...
        Set<String> storeEvents = ImmutableSet.of(Events.STORE_INIT,
                                                  Events.STORE_CLEAR,
                                                  Events.STORE_TRUNCATE);
        this.storeEventListener = event -> {
            if (storeEvents.contains(event.name())) {
                LOG.debug("Graph {} clear cache on event '{}'",
                          this.graph(), event.name());
                this.idCache.clear();
                this.nameCache.clear();
                this.cachedTypes.clear();
                return true;
            }
            return false;
        };
        this.store().provider().listen(this.storeEventListener);

        // Listen cache event: "cache"(invalid cache item)
        this.cacheEventListener = event -> {
            LOG.debug("Graph {} received cache event: {}",
                      this.graph(), event);
            event.checkArgs(String.class, Id.class);
            Object[] args = event.args();
            if (args[0].equals("invalid")) {
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
            }
            return false;
        };
        EventHub schemaEventHub = this.graph().schemaEventHub();
        if (!schemaEventHub.containsListener(Events.CACHE)) {
            schemaEventHub.listen(Events.CACHE, this.cacheEventListener);
        }
    }

    private void unlistenChanges() {
        // Unlisten store event
        this.store().provider().unlisten(this.storeEventListener);

        // Unlisten cache event
        EventHub schemaEventHub = this.graph().schemaEventHub();
        schemaEventHub.unlisten(Events.CACHE, this.cacheEventListener);
    }

    private void resetCachedAllIfReachedCapacity() {
        if (this.idCache.size() >= this.idCache.capacity()) {
            LOG.warn("Schema cache reached capacity({}): {}",
                     this.idCache.capacity(), this.idCache.size());
            this.cachedTypes.clear();
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
                this.resetCachedAllIfReachedCapacity();

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
                this.resetCachedAllIfReachedCapacity();

                this.nameCache.update(prefixedName, value);

                SchemaElement schema = (SchemaElement) value;
                Id prefixedId = generateId(schema.type(), schema.id());
                this.idCache.update(prefixedId, schema);
            }
        }
        return value;
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
        Boolean cachedAll = this.cachedTypes.getOrDefault(type, false);
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
            if (results.size() < free) {
                // Update cache
                for (T schema : results) {
                    Id prefixedId = generateId(schema.type(), schema.id());
                    this.idCache.update(prefixedId, schema);

                    Id prefixedName = generateId(schema.type(), schema.name());
                    this.nameCache.update(prefixedName, schema);
                }
                this.cachedTypes.putIfAbsent(type, true);
            }
            return results;
        }
    }
}
