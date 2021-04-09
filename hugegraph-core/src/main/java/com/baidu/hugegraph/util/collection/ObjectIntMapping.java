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
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.perf.PerfUtil.Watched;

public final class ObjectIntMapping {

    private static final int MAGIC = 1 << 16;
    private final IntObjectHashMap int2IdMap;

    public ObjectIntMapping() {
        this.int2IdMap = new IntObjectHashMap();
    }

    @Watched
    public int object2Code(Object object) {
        int key = object.hashCode();
        for (int i = 1; i > 0; i <<= 1) {
            for (int j = 0; i >= MAGIC && j < 10; j++) {
                Id existed = (Id) this.int2IdMap.get(key);
                if (existed == null) {
                    this.int2IdMap.put(key, object);
                    return key;
                }
                if (existed.equals(object)) {
                    return key;
                }
                key = key + i + j;
            }
        }
        throw new HugeException("Failed to get code for id: %s", object);
    }

    @Watched
    public Object code2Object(int code) {
        return this.int2IdMap.get(code);
    }
}
