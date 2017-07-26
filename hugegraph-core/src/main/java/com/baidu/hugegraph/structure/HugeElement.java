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

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGeneratorFactory;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.type.schema.PropertyKey;
import com.baidu.hugegraph.type.schema.VertexLabel;
import com.baidu.hugegraph.util.E;

public abstract class HugeElement implements Element, GraphType {

    protected final HugeGraph graph;
    protected boolean removed;
    protected Id id;
    protected Map<String, HugeProperty<?>> properties;

    public HugeElement(final HugeGraph graph, final Id id) {
        this.graph = graph;
        this.id = id;
        this.properties = new HashMap<>();
        this.removed = false;
    }

    public abstract GraphTransaction tx();

    protected abstract <V> HugeProperty<V> newProperty(PropertyKey pk, V v);

    @Override
    public Id id() {
        return this.id;
    }

    @Override
    public HugeGraph graph() {
        return this.graph;
    }

    public boolean removed() {
        return this.removed;
    }

    public Map<String, HugeProperty<?>> getProperties() {
        return Collections.unmodifiableMap(this.properties);
    }

    public void setProperties(Map<String, HugeProperty<?>> properties) {
        this.properties = properties;
    }

    @SuppressWarnings("unchecked")
    public <V> HugeProperty<V> getProperty(String key) {
        return (HugeProperty<V>) this.properties.get(key);
    }

    public boolean hasProperty(String key) {
        return this.properties.containsKey(key);
    }

    public boolean hasProperties() {
        return this.properties.size() > 0;
    }

    public <V> void setProperty(HugeProperty<V> prop) {
        this.properties.put(prop.key(), prop);
    }

    public <V> void removeProperty(String key) {
        this.properties.remove(key);
    }

    // SuppressWarnings for (HugeProperty) propSet/propList
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <V> HugeProperty<V> addProperty(String key, V value) {
        HugeProperty<V> prop = null;
        PropertyKey pkey = this.graph.schema().propertyKey(key);
        switch (pkey.cardinality()) {
            case SINGLE:
                /*
                 * NOTE: we don't support updating property(SINGLE), please use
                 * remove() + add() instead.
                 */
                E.checkArgument(!this.hasProperty(key),
                                "Not support updating property value");
                prop = this.newProperty(pkey, value);
                this.setProperty(prop);
                break;
            case SET:
                E.checkArgument(pkey.checkDataType(value),
                                "Invalid property value '%s' for key '%s'",
                                value, key);
                HugeProperty<Set<V>> propSet;
                if (this.hasProperty(key)) {
                    propSet = this.<Set<V>>getProperty(key);
                } else {
                    propSet = this.newProperty(pkey, new LinkedHashSet<V>());
                    this.setProperty(propSet);
                }

                propSet.value().add(value);

                // Any better ways?
                prop = (HugeProperty) propSet;
                break;
            case LIST:
                E.checkArgument(pkey.checkDataType(value),
                                "Invalid property value '%s' for key '%s'",
                                value, key);
                HugeProperty<List<V>> propList;
                if (!this.hasProperty(key)) {
                    propList = this.newProperty(pkey, new LinkedList<V>());
                    this.setProperty(propList);
                } else {
                    propList = this.<List<V>>getProperty(key);
                }

                propList.value().add(value);

                // Any better ways?
                prop = (HugeProperty) propList;
                break;
            default:
                assert false;
                break;
        }
        return prop;
    }

    public void resetProperties() {
        this.properties = new HashMap<>();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof HugeElement)) {
            return false;
        }

        HugeElement other = (HugeElement) obj;
        if (this.id() == null) {
            return false;
        }

        return this.id().equals(other.id());
    }

    @Override
    public int hashCode() {
        if (this.id() == null) {
            return super.hashCode();
        }
        return this.id().hashCode();
    }

    public static Id getIdValue(Object... keyValues) {
        Optional<Object> id = ElementHelper.getIdValue(keyValues);
        if (!id.isPresent()) {
            return null;
        }

        Object idValue = id.get();
        if (idValue instanceof Number) {
            // Number id
            return IdGeneratorFactory.generator().generate(
                    ((Number) idValue).longValue());
        } else if (idValue instanceof String) {
            // String id
            return IdGeneratorFactory.generator().generate((String) idValue);
        } else if (idValue instanceof Id) {
            // Id itself
            return (Id) idValue;
        } else if (idValue instanceof Element) {
            // Element
            return (Id) ((Element) idValue).id();
        }

        // Throw if error type
        throw new UnsupportedOperationException(
                  "Unsupported id type(must be a number or a string): " +
                  idValue.getClass().getSimpleName());
    }

    public static Object getLabelValue(Object... keyValues) {
        Object labelValue = null;
        for (int i = 0; i < keyValues.length; i = i + 2) {
            if (keyValues[i].equals(T.label)) {
                labelValue = keyValues[i + 1];
                E.checkArgument(labelValue instanceof String ||
                                labelValue instanceof VertexLabel,
                                "Expect a string or a VertexLabel object " +
                                "as the vertex label argument, but got: '%s'",
                                labelValue);
                if (labelValue instanceof String) {
                    ElementHelper.validateLabel((String) labelValue);
                }
            }
        }
        return labelValue;
    }
}
