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

import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.perf.PerfUtil.Watched;

public class SingleThreadObjectIntMapping<V> implements ObjectIntMapping<V> {

    private static final int MAGIC = 1 << 16;
    private static final int MAX_OFFSET = 10;

    private final IntObjectHashMap<V> int2IdMap;

    public SingleThreadObjectIntMapping() {
        this.int2IdMap = new IntObjectHashMap<>();
    }

    @Watched
    @SuppressWarnings("unchecked")
    public int object2Code(Object object) {
        int code = object.hashCode();
        // TODO: improve hash algorithm
        for (int i = 1; i > 0; i <<= 1) {
            for (int j = 0; j < MAX_OFFSET; j++) {
                V existed = this.int2IdMap.get(code);
                if (existed == null) {
                    this.int2IdMap.put(code, (V) object);
                    return code;
                }
                if (existed.equals(object)) {
                    return code;
                }
                code = code + i + j;
                /*
                 * If i < MAGIC, try (i * 2) to reduce conflicts, otherwise
                 * try (i + 1), (i + 2), ..., (i + 10) to try more times
                 * before try (i * 2).
                 */
                if (i < MAGIC) {
                    break;
                }
            }
        }
        throw new HugeException("Failed to get code for object: %s", object);
    }

    @Watched
    public V code2Object(int code) {
        return this.int2IdMap.get(code);
    }

    @Override
    public void clear() {
        this.int2IdMap.clear();
    }
}
