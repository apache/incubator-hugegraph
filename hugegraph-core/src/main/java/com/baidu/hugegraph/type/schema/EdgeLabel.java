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
import java.util.Set;

import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.EdgeLink;
import com.baidu.hugegraph.type.define.Frequency;


public abstract class EdgeLabel extends SchemaElement {

    public EdgeLabel(String name) {
        super(name);
    }

    @Override
    public HugeType type() {
        return HugeType.EDGE_LABEL;
    }

    @Override
    public EdgeLabel properties(String... propertyNames) {
        this.properties.addAll(Arrays.asList(propertyNames));
        return this;
    }

    public abstract boolean isDirected();

    public abstract Frequency frequency();

    public abstract EdgeLabel singleTime();

    public abstract EdgeLabel multiTimes();

    public abstract EdgeLabel link(String src, String tgt);

    public abstract Set<EdgeLink> links();

    public abstract void links(Set<EdgeLink> links);

    public abstract EdgeLabel sortKeys(String... keys);

    public abstract List<String> sortKeys();

    @Override
    public EdgeLabel ifNotExist() {
        this.checkExist = false;
        return this;
    }

    @Override
    public abstract EdgeLabel create();

    @Override
    public abstract EdgeLabel append();

    @Override
    public abstract EdgeLabel eliminate();

    @Override
    public abstract void remove();

    public abstract void rebuildIndex();

}
