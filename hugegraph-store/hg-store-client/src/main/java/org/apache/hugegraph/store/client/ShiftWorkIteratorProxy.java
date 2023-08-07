/*
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

package org.apache.hugegraph.store.client;

import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;

import org.apache.hugegraph.store.HgKvEntry;
import org.apache.hugegraph.store.HgKvIterator;
import org.apache.hugegraph.store.HgKvPagingIterator;

import lombok.extern.slf4j.Slf4j;

/**
 * created on 2021/10/24
 *
 * @version 0.1.1
 */
@Slf4j
public class ShiftWorkIteratorProxy implements HgKvIterator {
    private static final byte[] EMPTY_BYTES = new byte[0];
    private final int limit;
    private HgKvPagingIterator<HgKvEntry> iterator;
    private Queue<HgKvPagingIterator> queue = new LinkedList<>();
    private HgKvEntry entry;
    private int count;
    private int shiftCount;

    ShiftWorkIteratorProxy(List<HgKvPagingIterator> iterators, int limit) {
        this.queue = new LinkedList<>(iterators);
        this.limit = limit <= 0 ? Integer.MAX_VALUE : limit;
    }

    private HgKvPagingIterator getIterator() {
        if (this.queue.isEmpty()) {
            return null;
        }

        HgKvPagingIterator buf = null;

        while ((buf = this.queue.poll()) != null) {
            if (buf.hasNext()) {
                break;
            }
        }

        if (buf == null) {
            return null;
        }

        this.queue.add(buf);

        return buf;
    }

    private void closeIterators() {
        if (this.queue.isEmpty()) {
            return;
        }

        HgKvPagingIterator buf;

        while ((buf = this.queue.poll()) != null) {
            buf.close();
        }

    }

    private void setIterator() {

        //   if (++this.shiftCount >= this.iterator.getPageSize() / 2) {
        if (++this.shiftCount >= this.iterator.getPageSize()) {
            this.iterator = null;
            this.shiftCount = 0;
        }

    }

    private void doNext() {

    }

    @Override
    public byte[] key() {
        if (this.entry != null) {
            return this.entry.key();
        }
        return null;
    }

    @Override
    public byte[] value() {
        if (this.entry != null) {
            return this.entry.value();
        }
        return null;
    }

    @Override
    public byte[] position() {
        return this.iterator != null ? this.iterator.position() : EMPTY_BYTES;
    }

    @Override
    public void seek(byte[] position) {
        if (this.iterator != null) {
            this.iterator.seek(position);
        }
    }

    @Override
    public boolean hasNext() {
        if (this.count >= this.limit) {
            return false;
        }
        if (this.iterator == null
            || !this.iterator.hasNext()) {
            this.iterator = this.getIterator();
        }
        return this.iterator != null;
    }

    @Override
    public Object next() {
        if (this.iterator == null) {
            hasNext();
        }
        if (this.iterator == null) {
            throw new NoSuchElementException();
        }
        this.entry = this.iterator.next();
        this.setIterator();
        this.count++;
        //log.info("next - > {}",this.entry);
        return this.entry;
    }

    @Override
    public void close() {
        if (this.iterator != null) {
            this.iterator.close();
        }
        this.closeIterators();
    }
}
