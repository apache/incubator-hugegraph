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

package com.baidu.hugegraph.util.collection;

import java.util.AbstractSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.eclipse.collections.api.iterator.MutableLongIterator;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.iterator.ExtendableIterator;
import com.baidu.hugegraph.type.define.CollectionImplType;

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

public class IdSet extends AbstractSet<Id> {

    private LongHashSet numberIds;
    private Set<Id> nonNumberIds;

    public IdSet(CollectionImplType type) {
        this.numberIds = new LongHashSet();
        switch (type) {
            case JCF:
                this.nonNumberIds = new HashSet<>();
                break;
            case EC:
                this.nonNumberIds = new UnifiedSet<>();
                break;
            case FU:
                this.nonNumberIds = new ObjectOpenHashSet<>();
                break;
            default:
                throw new AssertionError(
                          "Unsupported collection type: " + type);
        }
    }

    @Override
    public int size() {
        return this.numberIds.size() + this.nonNumberIds.size();
    }

    @Override
    public boolean isEmpty() {
        return this.numberIds.isEmpty() && this.nonNumberIds.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        if (o instanceof IdGenerator.LongId) {
            return this.numberIds.contains(((IdGenerator.LongId) o).longValue());
        } else {
            return this.nonNumberIds.contains(o);
        }
    }

    @Override
    public Iterator<Id> iterator() {
        return new ExtendableIterator<>(
               this.nonNumberIds.iterator(),
               new EcLongIterator(this.numberIds.longIterator()));
    }

    @Override
    public boolean add(Id id) {
        if (id instanceof IdGenerator.LongId) {
            return this.numberIds.add(((IdGenerator.LongId) id).longValue());
        } else {
            return this.nonNumberIds.add(id);
        }
    }

    @Override
    public boolean remove(Object o) {
        if (o instanceof IdGenerator.LongId) {
            return this.numberIds.remove(((IdGenerator.LongId) o).longValue());
        } else {
            return this.nonNumberIds.remove(o);
        }
    }

    @Override
    public void clear() {
        this.numberIds.clear();
        this.nonNumberIds.clear();
    }

    private static class EcLongIterator implements Iterator<Id> {

        private final MutableLongIterator iterator;

        public EcLongIterator(MutableLongIterator iter) {
            this.iterator = iter;
        }

        @Override
        public boolean hasNext() {
            return this.iterator.hasNext();
        }

        @Override
        public Id next() {
            return IdGenerator.of(this.iterator.next());
        }

        @Override
        public void remove() {
            this.iterator.remove();
        }
    }
}
