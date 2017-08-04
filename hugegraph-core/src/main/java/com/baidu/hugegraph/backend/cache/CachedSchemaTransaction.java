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
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.baidu.hugegraph.backend.cache;

import java.util.List;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.event.EventHub;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.Events;
import com.google.common.collect.ImmutableList;

public class CachedSchemaTransaction extends SchemaTransaction {

    private final Cache cache;

    public CachedSchemaTransaction(HugeGraph graph, BackendStore store) {
        super(graph, store);
        this.cache = this.cache("schema");

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
        return IdGenerator.of(type.code() + "-" + name);
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
