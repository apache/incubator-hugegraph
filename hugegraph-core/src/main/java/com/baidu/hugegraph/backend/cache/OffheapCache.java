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
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import org.caffinitas.ohc.CacheSerializer;
import org.caffinitas.ohc.CloseableIterator;
import org.caffinitas.ohc.Eviction;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;

import com.baidu.hugegraph.HugeException;
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
import com.baidu.hugegraph.type.define.DataType;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.InsertionOrderUtil;

public class OffheapCache extends AbstractCache<Id, Object> {

    private final static long VALUE_SIZE_TO_SKIP = 100 * Bytes.KB;

    private final OHCache<Id, Value> cache;
    private final HugeGraph graph;
    private final AbstractSerializer serializer;

    public OffheapCache(HugeGraph graph, long capacity, long avgEntryBytes) {
        // NOTE: capacity unit is bytes, the super capacity expect elements size
        super(capacity);
        long capacityInBytes = capacity * (avgEntryBytes + 64L);
        if (capacityInBytes <= 0L) {
            capacityInBytes = 1L;
        }
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
    public boolean containsKey(Id id) {
        return this.cache.containsKey(id);
    }

    @Override
    protected Object access(Id id) {
        Value value = this.cache.get(id);
        return value == null ? null : value.value();
    }

    @Override
    protected boolean write(Id id, Object value, long timeOffset) {
        Value serializedValue = new Value(value);
        int serializedSize = serializedValue.serializedSize();
        if (serializedSize > VALUE_SIZE_TO_SKIP) {
            LOG.info("Skip to cache '{}' due to value size {} > limit {}",
                      id, serializedSize, VALUE_SIZE_TO_SKIP);
            return false;
        }
        long expireTime = this.expire();
        boolean success;
        if (expireTime <= 0L) {
             success = this.cache.put(id, serializedValue);
        } else {
            expireTime += now() + timeOffset;
            /*
             * Seems only the linked implementation support expiring entries,
             * the chunked implementation does not support it.
             */
            success = this.cache.put(id, serializedValue, expireTime);
        }
        assert success;
        return success;
    }

    @Override
    protected void remove(Id id) {
        this.cache.remove(id);
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
            this.value = this.deserialize(BytesBuffer.wrap(input));
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
                this.svalue.forReadWritten();
            }
            return this.svalue.asByteBuffer();
        }

        private void serialize(Object element, BytesBuffer buffer) {
            ValueType type = ValueType.valueOf(element);
            buffer.write(type.code());
            switch (type) {
                case LIST:
                    @SuppressWarnings("unchecked")
                    Collection<Object> list = (Collection<Object>) element;
                    serializeList(buffer, list);
                    break;
                case VERTEX:
                case EDGE:
                    serializeElement(buffer, type, element);
                    break;
                case UNKNOWN:
                    throw unsupported(this.value);
                default:
                    buffer.writeProperty(type.dataType(), element);
                    break;
            }
        }

        private Object deserialize(BytesBuffer buffer) {
            ValueType type = ValueType.valueOf(buffer.read());
            switch (type) {
                case LIST:
                    return deserializeList(buffer);
                case VERTEX:
                case EDGE:
                    return deserializeElement(type, buffer);
                case UNKNOWN:
                    throw unsupported(type);
                default:
                    return buffer.readProperty(type.dataType());
            }
        }

        private void serializeList(BytesBuffer buffer,
                                   Collection<Object> list) {
            // Write list
            buffer.writeVInt(list.size());
            for (Object i : list) {
                this.serialize(i, buffer);
            }
        }

        private List<Object> deserializeList(BytesBuffer buffer) {
            // Read list
            int length = buffer.readVInt();
            List<Object> list = InsertionOrderUtil.newList();
            for (int i = 0; i < length; i++) {
                list.add(this.deserialize(buffer));
            }
            return list;
        }

        private void serializeElement(BytesBuffer buffer,
                                      ValueType type, Object value) {
            E.checkNotNull(value, "serialize value");
            BackendEntry entry;
            if (type == ValueType.VERTEX) {
                entry = serializer().writeVertex((HugeVertex) value);
            } else if (type == ValueType.EDGE) {
                entry = serializer().writeEdge((HugeEdge) value);
            } else {
                throw unsupported(type);
            }

            assert entry.columnsSize() == 1;
            BackendColumn column = entry.columns().iterator().next();

            buffer.writeBytes(column.name);
            buffer.writeBigBytes(column.value);
        }

        private Object deserializeElement(ValueType type, BytesBuffer buffer) {
            byte[] key = buffer.readBytes();
            byte[] value = buffer.readBigBytes();
            BinaryBackendEntry entry;
            if (type == ValueType.VERTEX) {
                entry = new BinaryBackendEntry(HugeType.VERTEX, key);
                entry.column(key, value);
                return serializer().readVertex(graph(), entry);
            } else if (type == ValueType.EDGE) {
                entry = new BinaryBackendEntry(HugeType.EDGE, key);
                entry.column(key, value);
                return serializer().readEdge(graph(), entry);
            } else {
                throw unsupported(type);
            }
        }

        private HugeException unsupported(ValueType type) {
            throw new HugeException(
                      "Unsupported deserialize type: %s", type);
        }

        private HugeException unsupported(Object value) {
            throw new HugeException(
                      "Unsupported type of serialize value: '%s'(%s)",
                      value, value.getClass());
        }
    }

    private static enum ValueType {

        UNKNOWN,
        LIST,
        VERTEX,
        EDGE,
        BYTES(DataType.BLOB),
        STRING(DataType.TEXT),
        INT(DataType.INT),
        LONG(DataType.LONG),
        FLOAT(DataType.FLOAT),
        DOUBLE(DataType.DOUBLE),
        DATE(DataType.DATE);

        private DataType dataType;

        private ValueType() {
            this(DataType.UNKNOWN);
        }

        private ValueType(DataType dataType) {
            this.dataType = dataType;
        }

        public int code() {
            return this.ordinal();
        }

        public DataType dataType() {
            return this.dataType;
        }

        public static ValueType valueOf(int index) {
            ValueType[] values = values();
            E.checkArgument(0 <= index && index < values.length,
                            "Invalid ValueType index %s", index);
            return values[index];
        }

        public static ValueType valueOf(Object object) {
            E.checkNotNull(object, "object");
            Class<? extends Object> clazz = object.getClass();
            if (Collection.class.isAssignableFrom(clazz)) {
                return ValueType.LIST;
            } else if (clazz == HugeVertex.class) {
                return ValueType.VERTEX;
            } else if (clazz == HugeEdge.class) {
                return ValueType.EDGE;
            } else {
                for (ValueType type : values()) {
                    if (clazz == type.dataType().clazz()) {
                        return type;
                    }
                }
            }
            return ValueType.UNKNOWN;
        }
    }
}
