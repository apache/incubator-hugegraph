/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.backend.cache;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.util.E;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A cache that maps vertices to their associated edges using weak references.
 * This cache is designed to handle high write throughput and provides a way to
 * manage the lifecycle of cached edges.
 */
public class VertexToEdgeLookupCache extends AbstractCache<Id, Object> {

    private static final Logger LOG = LoggerFactory.getLogger(VertexToEdgeLookupCache.class);

    private final Map<Id, Set<Id>> index;
    public final Cache<Id, Object> edgesCache;

    public VertexToEdgeLookupCache(Cache<Id, Object> edgesCache, long capacity) {
        super(capacity);
        this.index = new ConcurrentHashMap<>((int) capacity);
        this.edgesCache = edgesCache;
    }

    @Override
    protected Object access(Id id) {
        E.checkNotNull(id, "Id");
        Set<Id> queryRefs = index.get(id);
        if (queryRefs == null) {
            return Set.of();
        }
        queryRefs.removeIf(ref -> !edgesCache.containsKey(ref));
        return queryRefs.stream()
                        .filter(Objects::nonNull)
                        .collect(Collectors.toSet());
    }

    @Override
    protected boolean write(Id id, Object value, long timeOffset) {
        E.checkNotNull(id, "id");
        E.checkNotNull(value, "value");
        index.compute(id, (k, queryRefs) -> {
            Set<Id> set = queryRefs != null ? queryRefs : ConcurrentHashMap.newKeySet();
            set.add((CachedBackendStore.QueryId) value);
            return set;
        });
        return true;
    }

    @Override
    protected void remove(Id id) {
        E.checkNotNull(id, "Id");
        removeQueriesByKey(id);
    }



    @Override
    protected Iterator<CacheNode<Id, Object>> nodes() {
        return null;
    }

    public Set<Id> snapshotKeys() {
        return Set.copyOf(index.keySet());
    }


    /**
     * Cleans up expired references and returns the total number of expired items.
     *
     * @return the total number of expired query IDs
     */
    public long tick() {
        LongAdder expireQueryIds = new LongAdder();
        LongAdder expireVertexIds = new LongAdder();

        // 根据数据量决定是否使用并行流
        Stream<Map.Entry<Id, Set<Id>>> stream = index.entrySet().stream();
        if (index.size() > 10_0000) {
            stream = stream.parallel();
        }

        stream.forEach(entry -> {
            Id id = entry.getKey();
            Set<Id> queryRefs = entry.getValue();
            queryRefs.removeIf(ref -> {
                if(!edgesCache.containsKey(ref)){
                    expireQueryIds.increment();
                    return true;
                }
                return false;
            });

            // 使用 compute 方法确保原子性
            index.compute(id, (key, value) -> {
                if (value == null || value.isEmpty()) {
                    expireVertexIds.increment();
                    return null; // 返回 null 表示移除该键
                }
                return value;
            });
        });

        if(expireQueryIds.longValue()>0){
            LOG.info("Expired Query IDs: {}, Expired Vertex/Edge IDs: {}",
                    expireQueryIds.longValue(), expireVertexIds.longValue());
        }
        return expireQueryIds.longValue();
    }




    /**
     * 把相应边缓存置为失效
     *
     * @param id the vertex ID to remove queries for
     */
    private void removeQueriesByKey(Id id) {
        E.checkNotNull(id, "Id");
        index.computeIfPresent(id, (k, queryIds) -> {
            queryIds.forEach(ref ->
                    edgesCache.invalidate(ref)
            );
            return null;
        });
    }

    @Override
    public void clear() {
        index.clear();
    }

    @Override
    public long size() {
        return index.size();
    }

    /**
     * Checks if the cache contains the given vertex ID.
     */
    public boolean containsKey(Id id) {
        E.checkNotNull(id, "vertexId");
        return index.containsKey(id);
    }

    @Override
    public void traverse(Consumer<Object> consumer) {
        // No-op
    }

    @Override
    public void traverseKeys(Consumer<Id> consumer) {
        E.checkNotNull(consumer, "consumer");
        this.index.keySet().forEach(consumer);
    }
}
