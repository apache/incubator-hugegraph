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
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.baidu.hugegraph.type.schema;

import java.util.Arrays;
import java.util.List;

import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.type.HugeType;


public abstract class VertexLabel extends SchemaElement {

    public VertexLabel(String name) {
        super(name);
    }

    @Override
    public HugeType type() {
        return HugeType.VERTEX_LABEL;
    }

    @Override
    public VertexLabel properties(String... propertyNames) {
        this.properties.addAll(Arrays.asList(propertyNames));
        return this;
    }

    public abstract List<String> primaryKeys();

    public abstract VertexLabel primaryKeys(String... keys);

    @Override
    public VertexLabel ifNotExist() {
        this.checkExist = false;
        return this;
    }

    @Override
    public abstract VertexLabel create();

    @Override
    public abstract VertexLabel append();

    @Override
    public abstract VertexLabel eliminate();

    @Override
    public abstract void remove();

    public abstract void rebuildIndex();
}
