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

package com.baidu.hugegraph.backend.store.ram;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import com.baidu.hugegraph.exception.NotSupportException;

public final class IntObjectMap<V> implements RamMap {

    private final Object[] array;

    public IntObjectMap(int size) {
        this.array = new Object[size];
    }

    @SuppressWarnings("unchecked")
    public V get(int key) {
        return (V) this.array[key];
    }

    public void set(int key, V value) {
        this.array[key] = value;
    }

    @Override
    public void clear() {
        Arrays.fill(this.array, null);
    }

    @Override
    public long size() {
        return this.array.length;
    }

    @Override
    public void writeTo(DataOutputStream buffer) throws IOException {
        throw new NotSupportException("IntObjectMap.writeTo");
    }

    @Override
    public void readFrom(DataInputStream buffer) throws IOException {
        throw new NotSupportException("IntObjectMap.readFrom");
    }
}
