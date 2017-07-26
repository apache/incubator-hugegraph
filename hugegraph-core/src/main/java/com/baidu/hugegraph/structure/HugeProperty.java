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

package com.baidu.hugegraph.structure;

import java.util.NoSuchElementException;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.schema.PropertyKey;
import com.baidu.hugegraph.util.E;

public abstract class HugeProperty<V> implements Property<V>, GraphType {

    protected final HugeElement owner;
    protected final PropertyKey key;
    protected final V value;

    public HugeProperty(HugeElement owner, PropertyKey key, V value) {
        E.checkArgument(owner != null, "Property owner can't be null");
        E.checkArgument(key != null, "Property key can't be null");
        this.owner = owner;
        this.key = key;
        this.value = value;

        E.checkArgument(key.checkValue(value),
                        "Invalid property value '%s' for key '%s', " +
                        "expect '%s', actual '%s'",
                        value, key.name(),
                        key.clazz().getSimpleName(),
                        value.getClass().getSimpleName());
    }

    public PropertyKey propertyKey() {
        return this.key;
    }

    @Override
    public HugeType type() {
        return HugeType.PROPERTY;
    }

    @Override
    public String name() {
        return this.key.name();
    }

    @Override
    public String key() {
        return this.key.name();
    }

    @Override
    public V value() throws NoSuchElementException {
        return this.value;
    }

    @Override
    public boolean isPresent() {
        return null != this.value;
    }

    @Override
    public HugeElement element() {
        return this.owner;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof HugeProperty)) {
            return false;
        }

        HugeProperty<?> other = (HugeProperty<?>) obj;
        return this.owner.equals(other.owner) && this.key.equals(other.key);
    }

    @Override
    public int hashCode() {
        return this.owner.hashCode() ^ this.key.hashCode();
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }
}
