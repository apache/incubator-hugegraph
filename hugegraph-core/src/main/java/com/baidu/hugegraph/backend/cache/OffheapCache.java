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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import org.caffinitas.ohc.CacheSerializer;
import org.caffinitas.ohc.CloseableIterator;
import org.caffinitas.ohc.Eviction;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.serializer.AbstractSerializer;
import com.baidu.hugegraph.backend.serializer.BinaryBackendEntry;
import com.baidu.hugegraph.backend.serializer.BinarySerializer;
import com.baidu.hugegraph.backend.serializer.BytesBuffer;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntry.BackendColumn;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.InsertionOrderUtil;

public class OffheapCache extends AbstractCache {

    private final OHCache<Id, Object> cache;
    private final HugeGraph graph;

    public OffheapCache(HugeGraph graph, int capacity) {
        super(capacity);
        this.graph = graph;
        this.cache = this.builder().capacity(capacity).build();
    }

    private HugeGraph graph() {
        return this.graph;
    }

    @Override
    public void traverse(Consumer<Object> consumer) {
        CloseableIterator<Id> iter = this.cache.keyIterator();
        while (iter.hasNext()) {
            Id key = iter.next();
            Object value = this.cache.get(key);
            consumer.accept(value);
        }
    }

    @Override
    public void clear() {
        this.cache.clear();
    }

    @Override
    public long size() {
        return this.cache.size();
    }

    @Override
    protected Object access(Id id) {
        return this.cache.get(id);
    }

    @Override
    protected void write(Id id, Object value) {
        this.cache.put(id, value);
    }

    @Override
    protected void remove(Id id) {
        this.cache.remove(id);
    }

    @Override
    protected boolean containsKey(Id id) {
        return this.cache.containsKey(id);
    }

    @Override
    protected <K, V> Iterator<CacheNode<K, V>> nodes() {
        // No needed to expire by timer, return none
        return Collections.emptyIterator();
    }

    private OHCacheBuilder<Id, Object> builder() {
        return OHCacheBuilder.<Id, Object>newBuilder()
                             .keySerializer(new IdSerializer())
                             .valueSerializer(new ValueSerializer())
                             .eviction(Eviction.LRU);
    }

//    private OHCacheBuilder<byte[], byte[]> builder() {
//        return OHCacheBuilder.<byte[], byte[]>newBuilder()
//                             .keySerializer(new BytesSerializer())
//                             .valueSerializer(new BytesSerializer())
//                             .eviction(Eviction.LRU);
//    }

//    static class BytesSerializer implements CacheSerializer<byte[]> {
//
//        @Override
//        public byte[] deserialize(ByteBuffer input) {
//            return BytesBuffer.wrap(input).readBytes();
//        }
//
//        @Override
//        public void serialize(byte[] bytes, ByteBuffer output) {
//            BytesBuffer.wrap(output).writeBytes(bytes);
//        }
//
//        @Override
//        public int serializedSize(byte[] bytes) {
//            return bytes.length;
//        }
//    }

    private class IdSerializer implements CacheSerializer<Id> {

        @Override
        public Id deserialize(ByteBuffer input) {
            return BytesBuffer.wrap(input).readId(true);
        }

        @Override
        public void serialize(Id id, ByteBuffer output) {
            BytesBuffer.wrap(output).writeId(id, true);
        }

        @Override
        public int serializedSize(Id id) {
            // TODO improve
            return id.length() + 2;
        }
    }

    private class ValueSerializer implements CacheSerializer<Object> {

        private final AbstractSerializer serializer;

        public ValueSerializer() {
            this.serializer = new BinarySerializer();
        }

        @Override
        public Object deserialize(ByteBuffer input) {
            BytesBuffer buffer = BytesBuffer.wrap(input);
            if (buffer.peek() == 0) {
                // Read list
                buffer.read();
                int length = buffer.readVInt();
                List<Object> list = InsertionOrderUtil.newList();
                for (int i = 0; i < length; i++) {
                    list.add(this.deserialize(input));
                }
                return list;
            }
            return this.deserialize(HugeType.fromCode(buffer.read()),
                                    buffer.readBytes(), buffer.readBytes());
        }

        @Override
        public void serialize(Object element, ByteBuffer output) {
            if (element instanceof List) {
                @SuppressWarnings("unchecked")
                List<Object> list = (List<Object>) element;
                // Write list
                BytesBuffer buffer = BytesBuffer.wrap(output);
                buffer.write(0);
                buffer.writeVInt(list.size());
                for (Object i : list) {
                    this.serialize(i, output);
                }
                return;
            }

            BackendEntry entry = this.serialize(element);
            BackendColumn column = oneColumn(entry);
            BytesBuffer buffer = BytesBuffer.wrap(output);
            buffer.write(entry.type().code());
            buffer.writeBytes(column.name);
            buffer.writeBytes(column.value);
        }

        @Override
        public int serializedSize(Object element) {
            // TODO improve
//            BackendColumn column = oneColumn(this.serialize(element));
//            return column.name.length + column.value.length;
            return 64;
        }

        private BackendEntry serialize(Object value) {
            BackendEntry entry;
            if (value instanceof HugeVertex) {
                entry = this.serializer.writeVertex((HugeVertex) value);
            } else if (value instanceof HugeEdge) {
                entry = this.serializer.writeEdge((HugeEdge) value);
            } else {
                throw new AssertionError("Invalid type of value: " + value);
            }
            assert entry.columnsSize() == 1;
            return entry;
        }

        private Object deserialize(HugeType type, byte[] key, byte[] value) {
            BinaryBackendEntry entry = new BinaryBackendEntry(type, key);
            entry.column(key, value);
            if (type.isVertex()) {
                return this.serializer.readVertex(graph(), entry);
            } else if (type.isEdge()) {
                return this.serializer.readEdge(graph(), entry);
            } else {
                throw new AssertionError("Invalid type: " + type);
            }
        }

        private BackendColumn oneColumn(BackendEntry entry) {
            return entry.columns().iterator().next();
        }
    }
}
