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
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.InsertionOrderUtil;

public class OffheapCache extends AbstractCache<Id, Object> {

    private final OHCache<Id, Value> cache;
    private final HugeGraph graph;
    private final AbstractSerializer serializer;

    public OffheapCache(HugeGraph graph, long capacityInBytes) {
        // NOTE: capacity unit is bytes, the super capacity expect elements size
        super(capacityInBytes);
        this.graph = graph;
        this.cache = this.builder().capacity(capacityInBytes).build();
        this.serializer = new BinarySerializer();
    }

    private HugeGraph graph() {
        return this.graph;
    }

    private AbstractSerializer serializer() {
        return this.serializer;
    }

    @Override
    public void traverse(Consumer<Object> consumer) {
        CloseableIterator<Id> iter = this.cache.keyIterator();
        while (iter.hasNext()) {
            Id key = iter.next();
            Value value = this.cache.get(key);
            consumer.accept(value.value());
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
        Value value = this.cache.get(id);
        return value == null ? null : value.value();
    }

    @Override
    protected void write(Id id, Object value) {
        long expireTime = this.expire();
        if (expireTime <= 0) {
            this.cache.put(id, new Value(value));
        } else {
            expireTime += now();
            /*
             * Seems only the linked implementation support expiring entries,
             * the chunked implementation does not support it.
             */
            this.cache.put(id, new Value(value), expireTime);
        }
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
    protected Iterator<CacheNode<Id, Object>> nodes() {
        // No needed to expire by timer, return none. use OHCache TTL instead
        return Collections.emptyIterator();
    }

    private OHCacheBuilder<Id, Value> builder() {
        return OHCacheBuilder.<Id, Value>newBuilder()
                             .keySerializer(new IdSerializer())
                             .valueSerializer(new ValueSerializer())
                             .eviction(Eviction.LRU)
                             .throwOOME(true)
                             .timeouts(true);
    }

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
            // NOTE: return size must be == actual bytes to write
            return BytesBuffer.allocate(id.length() + 2)
                              .writeId(id, true).position();
        }
    }

    private class ValueSerializer implements CacheSerializer<Value> {

        @Override
        public Value deserialize(ByteBuffer input) {
            return new Value(input);
        }

        @Override
        public void serialize(Value value, ByteBuffer output) {
            output.put(value.asBuffer());
        }

        @Override
        public int serializedSize(Value value) {
            // NOTE: return size must be >= actual bytes to write
            return value.serializedSize();
        }
    }

    private class Value {

        private final Object value;
        private BytesBuffer svalue = null;
        private int serializedSize = 0;

        public Value(Object value) {
            E.checkNotNull(value, "value");
            this.value = value;
        }

        public Value(ByteBuffer input) {
            this.value = this.deserialize(input);
        }

        public Object value() {
            return this.value;
        }

        public int serializedSize() {
            this.asBuffer();
            return this.serializedSize;
        }

        public ByteBuffer asBuffer() {
            if (this.svalue == null) {
                int listSize = 1;
                if (this.value instanceof List) {
                    listSize = ((List<?>) this.value).size();
                }
                this.svalue = BytesBuffer.allocate(64 * listSize);
                this.serialize(this.value, this.svalue);
                this.serializedSize = this.svalue.position();
                this.svalue.flip();
            }
            return this.svalue.asByteBuffer();
        }

        private void serialize(Object element, BytesBuffer buffer) {
            if (element instanceof List) {
                @SuppressWarnings("unchecked")
                List<Object> list = (List<Object>) element;
                // Write list
                buffer.write(0);
                buffer.writeVInt(list.size());
                for (Object i : list) {
                    this.serialize(i, buffer);
                }
                return;
            }

            BackendEntry entry = this.serialize(element);
            BackendColumn column = oneColumn(entry);
            buffer.write(entry.type().code());
            buffer.writeBytes(column.name);
            buffer.writeBytes(column.value);
        }

        private Object deserialize(ByteBuffer input) {
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

        private BackendEntry serialize(Object value) {
            BackendEntry entry;
            if (value instanceof HugeVertex) {
                entry = serializer().writeVertex((HugeVertex) value);
            } else if (value instanceof HugeEdge) {
                entry = serializer().writeEdge((HugeEdge) value);
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
                return serializer().readVertex(graph(), entry);
            } else if (type.isEdge()) {
                return serializer().readEdge(graph(), entry);
            } else {
                throw new AssertionError("Invalid type: " + type);
            }
        }

        private BackendColumn oneColumn(BackendEntry entry) {
            return entry.columns().iterator().next();
        }
    }
}
