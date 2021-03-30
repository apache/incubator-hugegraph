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

package com.baidu.hugegraph.traversal.algorithm;

import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectLongHashMap;

import com.baidu.hugegraph.backend.id.Id;

public class IdMapping {

    private LongObjectHashMap long2IdMap;
    private ObjectLongHashMap id2LongMap;
    private long nextLong;

    public IdMapping() {
        this.long2IdMap = new LongObjectHashMap();
        this.id2LongMap = new ObjectLongHashMap();
        this.nextLong = 0L;
    }

    public long getLong(Id id) {
        if (this.id2LongMap.containsKey(id)) {
            return this.id2LongMap.get(id);
        }
        this.nextLong++;
        this.id2LongMap.put(id, this.nextLong);
        this.long2IdMap.put(this.nextLong, id);
        return this.nextLong;
    }

    public Id getId(long code) {
        return (Id) this.long2IdMap.get(code);
    }
}
