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

package com.baidu.hugegraph.backend.store;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.InsertionOrderUtil;
import com.google.common.collect.ImmutableMap;

public class TableDefine {

    private final Map<HugeKeys, String> columns;
    private final List<HugeKeys> keys;
    private final Map<String, String> typesMapping;

    public TableDefine() {
        this.columns = InsertionOrderUtil.newMap();
        this.keys = InsertionOrderUtil.newList();
        this.typesMapping = ImmutableMap.of();
    }

    public TableDefine(Map<String, String> typesMapping) {
        this.columns = InsertionOrderUtil.newMap();
        this.keys = InsertionOrderUtil.newList();
        this.typesMapping = typesMapping;
    }

    public TableDefine column(HugeKeys key, String... desc) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < desc.length; i++) {
            String type = desc[i];
            // The first element of 'desc' is column data type, which may be
            // mapped to actual data type supported by backend store
            if (i == 0 && this.typesMapping.containsKey(type)) {
                type = this.typesMapping.get(type);
            }
            assert type != null;
            sb.append(type);
            if (i != desc.length - 1) {
                sb.append(" ");
            }
        }
        this.columns.put(key, sb.toString());
        return this;
    }

    public Map<HugeKeys, String> columns() {
        return Collections.unmodifiableMap(this.columns);
    }

    public Set<HugeKeys> columnNames() {
        return this.columns.keySet();
    }

    public Collection<String> columnTypes() {
        return this.columns.values();
    }

    public void keys(HugeKeys... keys) {
        this.keys.addAll(Arrays.asList(keys));
    }

    public List<HugeKeys> keys() {
        return Collections.unmodifiableList(this.keys);
    }
}
