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
import java.util.function.BiFunction;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntry.BackendIterator;
import com.baidu.hugegraph.backend.store.BackendEntryIterator;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.E;

public class BinaryEntryIterator<Elem> extends BackendEntryIterator {

    protected final BackendIterator<Elem> results;
    protected final BiFunction<BackendEntry, Elem, BackendEntry> merger;

    protected BackendEntry next;

    public BinaryEntryIterator(BackendIterator<Elem> results, Query query,
                               BiFunction<BackendEntry, Elem, BackendEntry> m) {
        super(query);

        E.checkNotNull(results, "results");
        E.checkNotNull(m, "merger");

        this.results = results;
        this.merger = m;
        this.next = null;

        this.skipOffset();

        if (query.paging()) {
            this.skipPageOffset(query.page());
        }
    }

    @Override
    public void close() throws Exception {
        this.results.close();
    }

    @Override
    protected final boolean fetch() {
        assert this.current == null;
        if (this.next != null) {
            this.current = this.next;
            this.next = null;
        }

        while (this.results.hasNext()) {
            Elem elem = this.results.next();
            BackendEntry merged = this.merger.apply(this.current, elem);
            E.checkState(merged != null, "Error when merging entry");
            if (this.current == null) {
                // The first time to read
                this.current = merged;
            } else if (merged == this.current) {
                // The next entry belongs to the current entry
                assert this.current != null;
            } else {
                // New entry
                assert this.next == null;
                this.next = merged;
                break;
            }

            // When limit exceed, stop fetching
            if (this.query.reachLimit(this.fetched())) {
                // Use next() to set page position if paging
                if (this.query.paging() && this.results.hasNext()) {
                    this.results.next();
                }
                this.results.close();
                break;
            }
        }

        return this.current != null;
    }

    @Override
    protected final long sizeOf(BackendEntry entry) {
        /*
         * One edge per column (one entry <==> a vertex),
         * or one element id per column (one entry <==> an index)
         */
        if (entry.type().isEdge() || entry.type().isIndex()) {
            return entry.columnsSize();
        }
        return 1L;
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
        byte[] position = this.results.position();
        if (position == null) {
            return null;
        }
        PageState page = new PageState(position, 0);
        return page.toString();
    }

    private void skipPageOffset(String page) {
        PageState pagestate = PageState.fromString(page);
        if (pagestate.offset() > 0 && this.fetch()) {
            this.skip(this.current, pagestate.offset());
        }
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
