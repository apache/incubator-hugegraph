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

import java.util.Base64;
import java.util.function.Function;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntry.BackendColumn;
import com.baidu.hugegraph.backend.store.BackendEntry.BackendColumnIterator;
import com.baidu.hugegraph.backend.store.BackendEntryIterator;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.E;

public class BinaryEntryIterator extends BackendEntryIterator<BackendColumn> {

    private final BackendColumnIterator columns;
    private final Function<BackendColumn, BackendEntry> entryCreater;

    private long remaining;
    private BackendEntry next;

    public BinaryEntryIterator(BackendColumnIterator columns, Query query,
                               Function<BackendColumn, BackendEntry> entry) {
        super(query);

        E.checkNotNull(columns, "columns");
        E.checkNotNull(entry, "entry");

        this.columns = columns;
        this.entryCreater = entry;
        this.remaining = query.limit();
        this.next = null;

        this.skipOffset();

        if (query.paging()) {
            this.skipPageOffset(query.page());
        }
    }

    private void skipPageOffset(String page) {
        PageState pagestate = PageState.fromString(page);
        if (pagestate.offset() > 0 && this.fetch()) {
            this.skip(this.current, pagestate.offset());
        }
    }

    @Override
    public void close() throws Exception {
        this.columns.close();
    }

    @Override
    protected final boolean fetch() {
        assert this.current == null;
        if (this.next != null) {
            this.current = this.next;
            this.next = null;
        }

        while (this.remaining > 0 && this.columns.hasNext()) {
            if (this.query.paging()) {
                this.remaining--;
            }
            BackendColumn col = this.columns.next();
            if (this.current == null) {
                // The first time to read
                this.current = this.entryCreater.apply(col);
                assert this.current != null;
                this.current.columns(col);
            } else if (this.current.belongToMe(col)) {
                // Does the column belongs to the current entry
                this.current.columns(col);
            } else {
                // New entry
                assert this.next == null;
                this.next = this.entryCreater.apply(col);
                assert this.next != null;
                this.next.columns(col);
                return true;
            }
        }

        return this.current != null;
    }

    @Override
    protected final long sizeOf(BackendEntry entry) {
        // One edge per column (entry <==> vertex)
        return entry.type() == HugeType.EDGE ? entry.columnsSize() : 1;
    }

    @Override
    protected final long skip(BackendEntry entry, long skip) {
        BinaryBackendEntry e = (BinaryBackendEntry) entry;
        E.checkState(e.columnsSize() > skip, "Invalid entry to skip");
        for (long i = 0; i < skip; i++) {
            e.removeColumn(0);
        }
        return e.columnsSize();
    }

    @Override
    protected String pageState() {
        byte[] position = this.columns.position();
        if (position == null) {
            return null;
        }
        PageState page = new PageState(position, 0);
        return page.toString();
    }

    public static class PageState {

        private final byte[] position;
        private final int offset;

        public PageState(byte[] position, int offset) {
            E.checkNotNull(position, "position");
            this.position = position;
            this.offset = offset;
        }

        public byte[] position() {
            return this.position;
        }

        public int offset() {
            return this.offset;
        }

        @Override
        public String toString() {
            return Base64.getEncoder().encodeToString(this.toBytes());
        }

        public byte[] toBytes() {
            int length = 2 + this.position.length + BytesBuffer.INT_LEN;
            BytesBuffer buffer = BytesBuffer.allocate(length);
            buffer.writeBytes(this.position);
            buffer.writeInt(this.offset);
            return buffer.bytes();
        }

        public static PageState fromString(String page) {
            byte[] bytes;
            try {
                bytes = Base64.getDecoder().decode(page);
            } catch (Exception e) {
                throw new BackendException("Invalid page: '%s'", e, page);
            }
            return fromBytes(bytes);
        }

        public static PageState fromBytes(byte[] bytes) {
            if (bytes.length == 0) {
                // The first page
                return new PageState(new byte[0], 0);
            }
            try {
                BytesBuffer buffer = BytesBuffer.wrap(bytes);
                return new PageState(buffer.readBytes(), buffer.readInt());
            } catch (Exception e) {
                throw new BackendException("Invalid page: '0x%s'",
                                           e, Bytes.toHex(bytes));
            }
        }
    }
}
