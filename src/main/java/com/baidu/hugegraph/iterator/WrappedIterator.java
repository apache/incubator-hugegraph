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

package com.baidu.hugegraph.iterator;

import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class WrappedIterator<R>
                implements Iterator<R>, AutoCloseable, Metadatable {

    protected R current;

    public WrappedIterator() {
        this.current = null;
    }

    @Override
    public boolean hasNext() {
        if (this.current != null) {
            return true;
        }
        return this.fetch();
    }

    @Override
    public R next() {
        if (this.current == null) {
            this.fetch();
        }
        if (this.current == null) {
            throw new NoSuchElementException();
        }
        R current = this.current;
        this.current = null;
        return current;
    }

    @Override
    public void close() throws Exception {
        Iterator<?> iterator = this.originIterator();
        if (iterator instanceof AutoCloseable) {
            ((AutoCloseable) iterator).close();
        }
    }

    @Override
    public Object metadata(String meta, Object... args) {
        Iterator<?> iterator = this.originIterator();
        if (iterator instanceof Metadatable) {
            return ((Metadatable) iterator).metadata(meta, args);
        }
        throw new IllegalStateException("Original iterator is not Metadatable");
    }

    protected abstract Iterator<?> originIterator();

    protected abstract boolean fetch();
}
