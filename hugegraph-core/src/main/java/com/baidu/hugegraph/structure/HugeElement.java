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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.perf.PerfUtil.Watched;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaLabel;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.CollectionUtil;
import com.baidu.hugegraph.util.E;

public abstract class HugeElement implements Element, GraphType {

    private final HugeGraph graph;

    protected Id id;
    protected Map<Id, HugeProperty<?>> properties;
    protected boolean removed;
    protected boolean fresh;
    protected boolean propLoaded;

    public HugeElement(final HugeGraph graph, Id id) {
        E.checkArgument(graph != null, "HugeElement graph can't be null");
        this.graph = graph;
        this.id = id;
        this.properties = new HashMap<>();
        this.removed = false;
        this.fresh = false;
        this.propLoaded = true;
    }

    public abstract SchemaLabel schemaLabel();

    protected abstract GraphTransaction tx();

    protected abstract <V> HugeProperty<V> newProperty(PropertyKey pk, V val);

    protected abstract <V> void onUpdateProperty(Cardinality cardinality,
                                                 HugeProperty<V> prop);

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

    public boolean fresh() {
        return this.fresh;
    }

    public boolean propLoaded() {
        return this.propLoaded;
    }

    public void propNotLoaded() {
        this.propLoaded = false;
    }

    public void committed() {
        this.fresh = false;
    }

    public Map<Id, HugeProperty<?>> getProperties() {
        return Collections.unmodifiableMap(this.properties);
    }

    public Map<Id, Object> getPropertiesMap() {
        Map<Id, Object> props = new HashMap<>();
        for (Map.Entry<Id, HugeProperty<?>> entry :
             this.properties.entrySet()) {
            props.put(entry.getKey(), entry.getValue().value());
        }
        return props;
    }

    @SuppressWarnings("unchecked")
    public <V> HugeProperty<V> getProperty(Id key) {
        return (HugeProperty<V>) this.properties.get(key);
    }

    public boolean hasProperty(Id key) {
        return this.properties.containsKey(key);
    }

    public boolean hasProperties() {
        return this.properties.size() > 0;
    }

    public int sizeOfProperties() {
        return this.properties.size();
    }

    @Watched(prefix = "element")
    public <V> HugeProperty<?> setProperty(HugeProperty<V> prop) {
        PropertyKey pkey = prop.propertyKey();
        E.checkArgument(pkey.checkValue(prop.value()),
                        "Invalid property value '%s' for key '%s', " +
                        "expect a value of type %s, actual type %s",
                        prop.value(), pkey.name(),
                        pkey.clazz().getSimpleName(),
                        prop.value().getClass().getSimpleName());
        return this.properties.put(pkey.id(), prop);
    }

    public <V> HugeProperty<?> removeProperty(Id key) {
        return this.properties.remove(key);
    }

    public <V> HugeProperty<V> addProperty(PropertyKey pkey, V value) {
        return this.addProperty(pkey, value, false);
    }

    @Watched(prefix = "element")
    public <V> HugeProperty<V> addProperty(PropertyKey pkey, V value,
                                           boolean notify) {
        HugeProperty<V> prop = null;
        switch (pkey.cardinality()) {
            case SINGLE:
                prop = this.newProperty(pkey, value);
                if (notify) {
                    /*
                     * NOTE: this method should be called before setProperty()
                     * because tx need to delete index without the new property
                     */
                    this.onUpdateProperty(pkey.cardinality(), prop);
                }
                this.setProperty(prop);
                break;
            case SET:
                prop = this.addPropertySet(pkey, value);
                if (notify) {
                    this.onUpdateProperty(pkey.cardinality(), prop);
                }
                break;
            case LIST:
                prop = this.addPropertyList(pkey, value);
                if (notify) {
                    this.onUpdateProperty(pkey.cardinality(), prop);
                }
                break;
            default:
                assert false;
                break;
        }
        return prop;
    }

    @Watched(prefix = "element")
    @SuppressWarnings({ "rawtypes", "unchecked" }) // (HugeProperty) propList
    private <V> HugeProperty<V> addPropertyList(PropertyKey pkey, V value) {
        HugeProperty<List<V>> propList;
        if (this.hasProperty(pkey.id())) {
            propList = this.getProperty(pkey.id());
        } else {
            propList = this.newProperty(pkey, new ArrayList<V>());
            this.setProperty(propList);
        }

        if (value instanceof List) {
            E.checkArgument(pkey.checkDataType((List) value),
                            "Invalid type of property values %s for key '%s'",
                            value, pkey.name());
            propList.value().addAll((List) value);
        } else if (value.getClass().isArray()) {
            List<V> valueList = CollectionUtil.toList(value);
            E.checkArgument(pkey.checkDataType(valueList),
                            "Invalid type of property values %s for key '%s'",
                            valueList, pkey.name());
            propList.value().addAll(valueList);
        } else {
            E.checkArgument(pkey.checkDataType(value),
                            "Invalid type of property value '%s' for key '%s'",
                            value, pkey.name());
            propList.value().add(value);
        }

        // Any better ways?
        return (HugeProperty) propList;
    }

    @Watched(prefix = "element")
    @SuppressWarnings({ "rawtypes", "unchecked" }) // (HugeProperty) propSet
    private <V> HugeProperty<V> addPropertySet(PropertyKey pkey, V value) {
        HugeProperty<Set<V>> propSet;
        if (this.hasProperty(pkey.id())) {
            propSet = this.getProperty(pkey.id());
        } else {
            propSet = this.newProperty(pkey, new HashSet<V>());
            this.setProperty(propSet);
        }

        if (value instanceof Set) {
            E.checkArgument(pkey.checkDataType((Set) value),
                            "Invalid type of property values %s for key '%s'",
                            value, pkey.name());
            propSet.value().addAll((Set) value);
        } else {
            E.checkArgument(pkey.checkDataType(value),
                            "Invalid type of property value '%s' for key '%s'",
                            value, pkey.name());
            propSet.value().add(value);
        }

        // Any better ways?
        return (HugeProperty) propSet;
    }

    public void resetProperties() {
        this.properties = new HashMap<>();
        this.propLoaded = false;
    }

    public void copyProperties(HugeElement element) {
        this.properties = new HashMap<>(element.properties);
        this.propLoaded = true;
    }

    public HugeElement copyAsFresh() {
        HugeElement elem = this.copy();
        elem.fresh = true;
        return elem;
    }

    public abstract HugeElement copy();

    public abstract Object sysprop(HugeKeys key);

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Element)) {
            return false;
        }

        Element other = (Element) obj;
        if (this.id() == null) {
            return false;
        }

        return this.id().equals(other.id());
    }

    @Override
    public int hashCode() {
        E.checkState(this.id() != null, "Element id can't be null");
        return ElementHelper.hashCode(this);
    }

    /**
     * Classify parameter list(pairs) from call request
     */
    @Watched(prefix = "element")
    public static ElementKeys classifyKeys(Object... keyValues) {
        ElementKeys elemKeys = new ElementKeys();

        if ((keyValues.length & 1) == 1) {
            throw Element.Exceptions.providedKeyValuesMustBeAMultipleOfTwo();
        }
        for (int i = 0; i < keyValues.length; i = i + 2) {
            Object key = keyValues[i];
            Object val = keyValues[i + 1];

            if (!(key instanceof String) && !(key instanceof T)) {
                throw Element.Exceptions
                      .providedKeyValuesMustHaveALegalKeyOnEvenIndices();
            }
            if (val == null) {
                throw Property.Exceptions.propertyValueCanNotBeNull();
            }

            if (key.equals(T.id)) {
                elemKeys.id = val;
            } else if (key.equals(T.label)) {
                elemKeys.label = val;
            } else {
                elemKeys.keys.add(key.toString());
            }
        }
        return elemKeys;
    }

    @Watched(prefix = "element")
    public static Id getIdValue(Object idValue) {
        if (idValue == null) {
            return null;
        }

        if (idValue instanceof String) {
            // String id
            return IdGenerator.of((String) idValue);
        } else if (idValue instanceof Id) {
            // Id itself
            return (Id) idValue;
        } else if (idValue instanceof Element) {
            // Element
            return (Id) ((Element) idValue).id();
        }

        // Throw if error type
        throw new UnsupportedOperationException(String.format(
                  "Invalid element id type: %s, must be a string",
                  idValue.getClass().getSimpleName()));
    }

    @Watched(prefix = "element")
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
                break;
            }
        }
        return labelValue;
    }

    public static class ElementKeys {

        private Object label = null;
        private Object id = null;
        private Set<String> keys = new HashSet<>();

        public Object label() {
            return this.label;
        }

        public void label(Object label) {
            this.label = label;
        }

        public Object id() {
            return this.id;
        }

        public void id(Object id) {
            this.id = id;
        }

        public Set<String> keys() {
            return this.keys;
        }

        public void keys(Set<String> keys) {
            this.keys = keys;
        }
    }
}
