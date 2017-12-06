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

package com.baidu.hugegraph.structure;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.E;

public class HugeEdgeProperty<V> extends HugeProperty<V> {

    public HugeEdgeProperty(HugeElement owner, PropertyKey key, V value) {
        super(owner, key, value);
    }

    @Override
    public HugeType type() {
        return HugeType.PROPERTY;
    }

    @Override
    public HugeEdge element() {
        assert this.owner instanceof HugeEdge;
        return (HugeEdge) this.owner;
    }

    @Override
    public void remove() {
        assert this.owner instanceof HugeEdge;
        EdgeLabel edgeLabel = ((HugeEdge) this.owner).schemaLabel();
        E.checkArgument(edgeLabel.nullableKeys().contains(
                        this.propertyKey().id()),
                        "Can't remove non-null edge property '%s'", this);
        this.owner.tx().removeEdgeProperty(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Property)) {
            return false;
        }
        return ElementHelper.areEqual(this, obj);
    }

    public HugeEdgeProperty<V> switchEdgeOwner() {
        assert this.owner instanceof HugeEdge;
        return new HugeEdgeProperty<V>(((HugeEdge) this.owner).switchOwner(),
                                       this.key, this.value);
    }
}
