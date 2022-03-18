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

package com.baidu.hugegraph.backend.serializer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntryIterator;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.E;

public class BinaryBackendEntry implements BackendEntry {

    private static final byte[] EMPTY_BYTES = new byte[]{};

    private final HugeType type;
    private final BinaryId id;
    private Id subId;
    private final List<BackendColumn> columns;
    private long ttl;
    private boolean olap;

    public BinaryBackendEntry(HugeType type, byte[] bytes) {
        this(type, BytesBuffer.wrap(bytes).parseId(type));
    }

    public BinaryBackendEntry(HugeType type, BinaryId id) {
        this.type = type;
        this.id = id;
        this.subId = null;
        this.columns = new ArrayList<>();
        this.ttl = 0L;
        this.olap = false;
    }

    @Override
    public HugeType type() {
        return this.type;
    }

    @Override
    public BinaryId id() {
        return this.id;
    }

    @Override
    public Id originId() {
        return this.id.origin();
    }

    @Override
    public Id subId() {
        return this.subId;
    }

    public void subId(Id subId) {
        this.subId = subId;
    }

    public void ttl(long ttl) {
        this.ttl = ttl;
    }

    @Override
    public long ttl() {
        return this.ttl;
    }

    public void olap(boolean olap) {
        this.olap = olap;
    }

    @Override
    public boolean olap() {
        return this.olap;
    }

    @Override
    public String toString() {
        return String.format("%s: %s", this.id, this.columns.toString());
    }

    public BackendColumn column(byte[] name) {
        for (BackendColumn col : this.columns) {
            if (Bytes.equals(col.name, name)) {
                return col;
            }
        }
        return null;
    }

    public void column(BackendColumn column) {
        this.columns.add(column);
    }

    public void column(byte[] name, byte[] value) {
        E.checkNotNull(name, "name");
        value = value != null ? value : EMPTY_BYTES;
        this.columns.add(BackendColumn.of(name, value));
    }

    @Override
    public Collection<BackendColumn> columns() {
        return Collections.unmodifiableList(this.columns);
    }

    @Override
    public int columnsSize() {
        return this.columns.size();
    }

    @Override
    public void columns(Collection<BackendColumn> bytesColumns) {
        this.columns.addAll(bytesColumns);
    }

    @Override
    public void columns(BackendColumn bytesColumn) {
        this.columns.add(bytesColumn);
        long maxSize = BackendEntryIterator.INLINE_BATCH_SIZE;
        if (this.columns.size() > maxSize) {
            E.checkState(false, "Too many columns in one entry: %s", maxSize);
        }
    }

    public BackendColumn removeColumn(int index) {
        return this.columns.remove(index);
    }

    @Override
    public void merge(BackendEntry other) {
        for (BackendColumn col : other.columns()) {
            BackendColumn origin = this.column(col.name);
            if (origin != null) {
                origin.value = col.value;
            } else {
                this.column(col);
            }
        }
    }

    @Override
    public boolean mergable(BackendEntry other) {
        if (!(other instanceof BinaryBackendEntry)) {
            return false;
        }
        if (!this.id().equals(other.id())) {
            return false;
        }
        this.columns(other.columns());
        return true;
    }

    @Override
    public void clear() {
        this.columns.clear();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof BinaryBackendEntry)) {
            return false;
        }
        BinaryBackendEntry other = (BinaryBackendEntry) obj;
        if (this.id() != other.id() && !this.id().equals(other.id())) {
            return false;
        }
        if (this.columns.size() != other.columns.size()) {
            return false;
        }
        if (!this.columns.containsAll(other.columns)) {
            return false;
        }
        return true;
    }

    protected static final class BinaryId implements Id {

        private final byte[] bytes;
        private final Id id;

        public BinaryId(byte[] bytes, Id id) {
            this.bytes = bytes;
            this.id = id;
        }

        public Id origin() {
            return this.id;
        }

        @Override
        public IdType type() {
            return IdType.UNKNOWN;
        }

        @Override
        public Object asObject() {
            return ByteBuffer.wrap(this.bytes);
        }

        @Override
        public String asString() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long asLong() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int compareTo(Id other) {
            return Bytes.compare(this.bytes, other.asBytes());
        }

        @Override
        public byte[] asBytes() {
            return this.bytes;
        }

        public byte[] asBytes(int offset) {
            E.checkArgument(offset < this.bytes.length,
                            "Invalid offset %s, must be < length %s",
                            offset, this.bytes.length);
            return Arrays.copyOfRange(this.bytes, offset, this.bytes.length);
        }

        @Override
        public int length() {
            return this.bytes.length;
        }

        @Override
        public int hashCode() {
            return ByteBuffer.wrap(this.bytes).hashCode();
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof BinaryId)) {
                return false;
            }
            return Arrays.equals(this.bytes, ((BinaryId) other).bytes);
        }

        @Override
        public String toString() {
            return "0x" + Bytes.toHex(this.bytes);
        }
    }
}
