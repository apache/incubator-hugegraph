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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.store.ram.IntObjectMap;
import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.event.EventHub;
import com.baidu.hugegraph.event.EventListener;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Events;
import com.google.common.collect.ImmutableSet;

public final class CachedSchemaTransaction extends SchemaTransaction {

    private final Cache<Id, Object> idCache;
    private final Cache<Id, Object> nameCache;

    private final SchemaCaches<SchemaElement> arrayCaches;

    private EventListener storeEventListener;
    private EventListener cacheEventListener;

    public CachedSchemaTransaction(HugeGraphParams graph, BackendStore store) {
        super(graph, store);

        final long capacity = graph.configuration()
                                   .get(CoreOptions.SCHEMA_CACHE_CAPACITY);
        this.idCache = this.cache("schema-id", capacity);
        this.nameCache = this.cache("schema-name", capacity);

        SchemaCaches<SchemaElement> attachment = this.idCache.attachment();
        if (attachment == null) {
            int acSize = (int) (capacity >> 3);
            attachment = this.idCache.attachment(new SchemaCaches<>(acSize));
        }
        this.arrayCaches = attachment;

        this.listenChanges();
    }

    @Override
    public void close() {
        try {
            super.close();
        } finally {
            this.clearCache(false);
            this.unlistenChanges();
        }
    }

    private Cache<Id, Object> cache(String prefix, long capacity) {
        final String name = prefix + "-" + this.graphName();
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
                LOG.debug("Graph {} clear schema cache on event '{}'",
                          this.graph(), event.name());
                this.clearCache(true);
                return true;
            }
            return false;
        };
        this.store().provider().listen(this.storeEventListener);

        // Listen cache event: "cache"(invalid cache item)
        this.cacheEventListener = event -> {
            LOG.debug("Graph {} received schema cache event: {}",
                      this.graph(), event);
            Object[] args = event.args();
            E.checkArgument(args.length > 0 && args[0] instanceof String,
                            "Expect event action argument");
            if (Cache.ACTION_INVALID.equals(args[0])) {
                event.checkArgs(String.class, HugeType.class, Id.class);
                HugeType type = (HugeType) args[1];
                Id id = (Id) args[2];
                this.arrayCaches.remove(type, id);

                id = generateId(type, id);
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
                this.resetCachedAll(type);
                return true;
            } else if (Cache.ACTION_CLEAR.equals(args[0])) {
                event.checkArgs(String.class, HugeType.class);
                this.clearCache(false);
                return true;
            }
            return false;
        };
        EventHub schemaEventHub = this.params().schemaEventHub();
        if (!schemaEventHub.containsListener(Events.CACHE)) {
            schemaEventHub.listen(Events.CACHE, this.cacheEventListener);
        }
    }

    private final void resetCachedAll(HugeType type) {
        // Set the cache all flag of the schema type to false
        this.cachedTypes().put(type, false);
    }

    private void clearCache(boolean notify) {
        this.idCache.clear();
        this.nameCache.clear();
        this.arrayCaches.clear();

        if (notify) {
            this.notifyChanges(Cache.ACTION_CLEARED, null, null);
        }
    }

    private void unlistenChanges() {
        // Unlisten store event
        this.store().provider().unlisten(this.storeEventListener);

        // Unlisten cache event
        EventHub schemaEventHub = this.params().schemaEventHub();
        schemaEventHub.unlisten(Events.CACHE, this.cacheEventListener);
    }

    private void notifyChanges(String action, HugeType type, Id id) {
        EventHub graphEventHub = this.params().schemaEventHub();
        graphEventHub.notify(Events.CACHE, action, type, id);
    }

    private final void resetCachedAllIfReachedCapacity() {
        if (this.idCache.size() >= this.idCache.capacity()) {
            LOG.warn("Schema cache reached capacity({}): {}",
                     this.idCache.capacity(), this.idCache.size());
            this.cachedTypes().clear();
        }
    }

    private final CachedTypes cachedTypes() {
        return this.arrayCaches.cachedTypes();
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

        // update id cache
        Id prefixedId = generateId(schema.type(), schema.id());
        this.idCache.update(prefixedId, schema);

        // update name cache
        Id prefixedName = generateId(schema.type(), schema.name());
        this.nameCache.update(prefixedName, schema);

        // update optimized array cache
        this.arrayCaches.updateIfNeeded(schema);

        this.notifyChanges(Cache.ACTION_INVALIDED, schema.type(), schema.id());
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <T extends SchemaElement> T getSchema(HugeType type, Id id) {
        // try get from optimized array cache
        if (id.number() && id.asLong() > 0L) {
            SchemaElement value = this.arrayCaches.get(type, id);
            if (value != null) {
                return (T) value;
            }
        }

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

        // update optimized array cache
        this.arrayCaches.updateIfNeeded((SchemaElement) value);

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

        // remove from optimized array cache
        this.arrayCaches.remove(schema.type(), schema.id());

        this.notifyChanges(Cache.ACTION_INVALIDED, schema.type(), schema.id());
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

    private static final class SchemaCaches<V extends SchemaElement> {

        private final int size;

        private final IntObjectMap<V> pks;
        private final IntObjectMap<V> vls;
        private final IntObjectMap<V> els;
        private final IntObjectMap<V> ils;

        private final CachedTypes cachedTypes;

        public SchemaCaches(int size) {
            // TODO: improve size of each type for optimized array cache
            this.size = size;

            this.pks = new IntObjectMap<>(size);
            this.vls = new IntObjectMap<>(size);
            this.els = new IntObjectMap<>(size);
            this.ils = new IntObjectMap<>(size);

            this.cachedTypes = new CachedTypes();
        }

        public void updateIfNeeded(V schema) {
            if (schema == null) {
                return;
            }
            Id id = schema.id();
            if (id.number() && id.asLong() > 0L) {
                this.set(schema.type(), id, schema);
            }
        }

        public V get(HugeType type, Id id) {
            assert id.number();
            long longId = id.asLong();
            if (longId <= 0L) {
                assert false : id;
                return null;
            }
            int key = (int) longId;
            if (key >= this.size) {
                return null;
            }
            switch (type) {
                case PROPERTY_KEY:
                    return this.pks.get(key);
                case VERTEX_LABEL:
                    return this.vls.get(key);
                case EDGE_LABEL:
                    return this.els.get(key);
                case INDEX_LABEL:
                    return this.ils.get(key);
                default:
                    return null;
            }
        }

        public void set(HugeType type, Id id, V value) {
            assert id.number();
            long longId = id.asLong();
            if (longId <= 0L) {
                assert false : id;
                return;
            }
            int key = (int) longId;
            if (key >= this.size) {
                return;
            }
            switch (type) {
                case PROPERTY_KEY:
                    this.pks.set(key, value);
                    break;
                case VERTEX_LABEL:
                    this.vls.set(key, value);
                    break;
                case EDGE_LABEL:
                    this.els.set(key, value);
                    break;
                case INDEX_LABEL:
                    this.ils.set(key, value);
                    break;
                default:
                    // pass
                    break;
            }
        }

        public void remove(HugeType type, Id id) {
            assert id.number();
            long longId = id.asLong();
            if (longId <= 0L) {
                return;
            }
            int key = (int) longId;
            V value = null;
            if (key >= this.size) {
                return;
            }
            switch (type) {
                case PROPERTY_KEY:
                    this.pks.set(key, value);
                    break;
                case VERTEX_LABEL:
                    this.vls.set(key, value);
                    break;
                case EDGE_LABEL:
                    this.els.set(key, value);
                    break;
                case INDEX_LABEL:
                    this.ils.set(key, value);
                    break;
                default:
                    // pass
                    break;
            }
        }

        public void clear() {
            this.pks.clear();
            this.vls.clear();
            this.els.clear();
            this.ils.clear();

            this.cachedTypes.clear();
        }

        public CachedTypes cachedTypes() {
            return this.cachedTypes;
        }
    }

    private static class CachedTypes
                   extends ConcurrentHashMap<HugeType, Boolean> {

        private static final long serialVersionUID = -2215549791679355996L;
    }
}
