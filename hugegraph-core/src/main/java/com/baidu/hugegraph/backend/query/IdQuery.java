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

package com.baidu.hugegraph.backend.query;

import java.util.LinkedHashSet;
import java.util.Set;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.type.HugeType;

public class IdQuery extends Query {

    // The ids will be concated with `or`
    private Set<Id> ids;

    public IdQuery(HugeType resultType) {
        super(resultType);
        this.ids = new LinkedHashSet<>();
    }

    public IdQuery(HugeType resultType, Set<Id> ids) {
        super(resultType);
        this.ids = new LinkedHashSet<>(ids);
    }

    public IdQuery(HugeType resultType, Id id) {
        this(resultType);
        this.query(id);
    }

    @Override
    public Set<Id> ids() {
        return this.ids;
    }

    public void resetIds() {
        this.ids = new LinkedHashSet<>();
    }

    public IdQuery query(Id id) {
        this.ids.add(id);
        return this;
    }

    @Override
    public boolean test(HugeElement element) {
        return ids.contains(element.id());
    }

    @Override
    public String toString() {
        return String.format("%s where id in %s",
                             super.toString(), this.ids.toString());
    }
}
