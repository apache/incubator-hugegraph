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

package org.apache.hugegraph.store.client.grpc;

import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.hugegraph.store.HgSeekAble;
import org.apache.hugegraph.store.client.util.HgAssert;

/**
 * 2022/3/11
 */
class SeekAbleIterator<E> implements Iterator, HgSeekAble {
    private final Iterator<E> iterator;
    private final Consumer<byte[]> seeker;
    private final Supplier<byte[]> positioner;

    private SeekAbleIterator(Iterator<E> iterator, Supplier<byte[]> positioner,
                             Consumer<byte[]> seeker) {
        this.iterator = iterator;
        this.positioner = positioner;
        this.seeker = seeker;
    }

    public static <E> SeekAbleIterator of(Iterator<E> iterator, Supplier<byte[]> positioner,
                                          Consumer<byte[]> seeker) {
        HgAssert.isArgumentNotNull(iterator, "iterator");
        HgAssert.isArgumentNotNull(positioner, "positioner");
        HgAssert.isArgumentNotNull(seeker, "seeker");
        return new SeekAbleIterator(iterator, positioner, seeker);
    }

    @Override
    public byte[] position() {
        return this.positioner.get();
    }

    @Override
    public void seek(byte[] position) {
        this.seeker.accept(position);
    }

    @Override
    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    @Override
    public E next() {
        return this.iterator.next();
    }
}