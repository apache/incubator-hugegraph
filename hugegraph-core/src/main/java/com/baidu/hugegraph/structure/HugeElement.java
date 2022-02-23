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
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.tuple.primitive.IntObjectPair;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.EdgeId;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.serializer.BytesBuffer;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.perf.PerfUtil.Watched;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaLabel;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.Idfiable;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.CollectionUtil;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.baidu.hugegraph.util.collection.CollectionFactory;

public abstract class HugeElement implements Element, GraphType, Idfiable {

    private static final Logger LOG = Log.logger(HugeElement.class);

    private static final MutableIntObjectMap<HugeProperty<?>> EMPTY_MAP =
                         CollectionFactory.newIntObjectMap();
    private static final int MAX_PROPERTIES = BytesBuffer.UINT16_MAX;

    private final HugeGraph graph;
    private MutableIntObjectMap<HugeProperty<?>> properties;

    private long expiredTime; // TODO: move into properties to keep small object

    private boolean removed;
    private boolean fresh;
    private boolean propLoaded;
    private boolean defaultValueUpdated;

    public HugeElement(final HugeGraph graph) {
//        E.checkArgument(graph != null, "HugeElement graph can't be null");
        this.graph = graph;
        this.properties = EMPTY_MAP;
        this.expiredTime = 0L;
        this.removed = false;
        this.fresh = false;
        this.propLoaded = true;
        this.defaultValueUpdated = false;
    }

    public abstract SchemaLabel schemaLabel();

    protected abstract GraphTransaction tx();

    protected abstract <V> HugeProperty<V> newProperty(PropertyKey pk, V val);

    protected abstract <V> void onUpdateProperty(Cardinality cardinality,
                                                 HugeProperty<V> prop);

    protected abstract boolean ensureFilledProperties(boolean throwIfNotExist);

    protected void updateToDefaultValueIfNone() {
        if (this.fresh() || this.defaultValueUpdated) {
            return;
        }
        this.defaultValueUpdated = true;
        // Set default value if needed
        for (Id pkeyId : this.schemaLabel().properties()) {
            if (this.properties.containsKey(intFromId(pkeyId))) {
                continue;
            }
            PropertyKey pkey = this.graph().propertyKey(pkeyId);
            Object value = pkey.defaultValue();
            if (value != null) {
                this.setProperty(this.newProperty(pkey, value));
            }
        }
        this.defaultValueUpdated = true;
    }

    @Override
    public HugeGraph graph() {
        return this.graph;
    }

    protected void removed(boolean removed) {
        this.removed = removed;
    }

    public boolean removed() {
        return this.removed;
    }

    protected void fresh(boolean fresh) {
        this.fresh = fresh;
    }

    public boolean fresh() {
        return this.fresh;
    }

    public boolean isPropLoaded() {
        return this.propLoaded;
    }

    protected void propLoaded() {
        this.propLoaded = true;
    }

    public void propNotLoaded() {
        this.propLoaded = false;
    }

    public void forceLoad() {
        this.ensureFilledProperties(false);
    }

    public void committed() {
        this.fresh = false;
        // Set expired time
        this.setExpiredTimeIfNeeded();
    }

    public void setExpiredTimeIfNeeded() {
        SchemaLabel label = this.schemaLabel();
        if (label.ttl() == 0L) {
            return;
        }
        long now = this.graph.now();
        if (SchemaLabel.NONE_ID.equals(label.ttlStartTime())) {
            this.expiredTime(now + label.ttl());
            return;
        }
        Date date = this.getPropertyValue(label.ttlStartTime());
        if (date == null) {
            this.expiredTime(now + label.ttl());
            return;
        }
        long expired = date.getTime() + label.ttl();
        E.checkArgument(expired > now,
                        "The expired time '%s' of '%s' is prior to now: %s",
                        new Date(expired), this, now);
        this.expiredTime(expired);
    }

    public long expiredTime() {
        return this.expiredTime;
    }

    public void expiredTime(long expiredTime) {
        this.expiredTime = expiredTime;
    }

    public boolean expired() {
        boolean expired;
        SchemaLabel label = this.schemaLabel();
        if (label.ttl() == 0L) {
            // No ttl, not expired
            return false;
        }
        if (this.expiredTime > 0L) {
            // Has ttl and set expiredTime properly
            expired = this.expiredTime < this.graph.now();
            LOG.debug("The element {} {} with expired time {} and now {}",
                      this, expired ? "expired" : "not expired",
                      this.expiredTime, this.graph.now());
            return expired;
        }
        // Has ttl, but failed to set expiredTime when insert
        LOG.error("The element {} should have positive expired time, " +
                  "but got {}! ttl is {} ttl start time is {}",
                  this, this.expiredTime, label.ttl(), label.ttlStartTimeName());
        if (SchemaLabel.NONE_ID.equals(label.ttlStartTime())) {
            // No ttlStartTime, can't decide whether timeout, treat not expired
            return false;
        }
        Date date = this.getPropertyValue(label.ttlStartTime());
        if (date == null) {
            // No ttlStartTime, can't decide whether timeout, treat not expired
            return false;
        }
        // Has ttlStartTime, re-calc expiredTime to decide whether timeout,
        long expiredTime = date.getTime() + label.ttl();
        expired = expiredTime < this.graph.now();
        LOG.debug("The element {} {} with expired time {} and now {}",
                  this, expired ? "expired" : "not expired",
                  expiredTime, this.graph.now());
        return expired;
    }

    public long ttl() {
        if (this.expiredTime == 0L || this.expiredTime < this.graph.now()) {
            return 0L;
        }
        return this.expiredTime - this.graph.now();
    }

    public boolean hasTtl() {
        return this.schemaLabel().ttl() > 0L;
    }

    // TODO: return MutableIntObjectMap<HugeProperty<?>>
    public Map<Id, HugeProperty<?>> getProperties() {
        Map<Id, HugeProperty<?>> props = new HashMap<>();
        for (IntObjectPair<HugeProperty<?>> e : this.properties.keyValuesView()) {
            props.put(IdGenerator.of(e.getOne()), e.getTwo());
        }
        return props;
    }

    // TODO: return MutableIntObjectMap<HugeProperty<?>>
    public Map<Id, HugeProperty<?>> getFilledProperties() {
        this.ensureFilledProperties(true);
        return this.getProperties();
    }

    // TODO: return MutableIntObjectMap<HugeProperty<?>>
    public Map<Id, Object> getPropertiesMap() {
        Map<Id, Object> props = new HashMap<>();
        for (IntObjectPair<HugeProperty<?>> e : this.properties.keyValuesView()) {
            props.put(IdGenerator.of(e.getOne()), e.getTwo().value());
        }
        return props;
    }

    // TODO: return MutableIntObjectMap<HugeProperty<?>>
    public Map<Id, HugeProperty<?>> getAggregateProperties() {
        Map<Id, HugeProperty<?>> aggrProps = new HashMap<>();
        for (IntObjectPair<HugeProperty<?>> e : this.properties.keyValuesView()) {
            if (e.getTwo().type().isAggregateProperty()) {
                aggrProps.put(IdGenerator.of(e.getOne()), e.getTwo());
            }
        }
        return aggrProps;
    }

    @SuppressWarnings("unchecked")
    public <V> HugeProperty<V> getProperty(Id key) {
        return (HugeProperty<V>) this.properties.get(intFromId(key));
    }

    @SuppressWarnings("unchecked")
    public <V> V getPropertyValue(Id key) {
        HugeProperty<?> prop = this.properties.get(intFromId(key));
        if (prop == null) {
            return null;
        }
        return (V) prop.value();
    }

    public boolean hasProperty(Id key) {
        return this.properties.containsKey(intFromId(key));
    }

    public boolean hasProperties() {
        return this.properties.size() > 0;
    }

    public int sizeOfProperties() {
        return this.properties.size();
    }

    public int sizeOfSubProperties() {
        int size = 0;
        for (HugeProperty<?> p : this.properties.values()) {
            size++;
            if (p.propertyKey().cardinality() != Cardinality.SINGLE &&
                p.value() instanceof Collection) {
                size += ((Collection<?>) p.value()).size();
            }
        }
        return size;
    }

    @Watched(prefix = "element")
    public <V> HugeProperty<?> setProperty(HugeProperty<V> prop) {
        if (this.properties == EMPTY_MAP) {
            this.properties = CollectionFactory.newIntObjectMap();
        }
        PropertyKey pkey = prop.propertyKey();

        E.checkArgument(this.properties.containsKey(intFromId(pkey.id())) ||
                        this.properties.size() < MAX_PROPERTIES,
                        "Exceeded the maximum number of properties");
        return this.properties.put(intFromId(pkey.id()), prop);
    }

    public <V> HugeProperty<?> removeProperty(Id key) {
        return this.properties.remove(intFromId(key));
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
                prop = this.addProperty(pkey, value, HashSet::new);
                if (notify) {
                    this.onUpdateProperty(pkey.cardinality(), prop);
                }
                break;
            case LIST:
                prop = this.addProperty(pkey, value, ArrayList::new);
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
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private <V> HugeProperty<V> addProperty(PropertyKey pkey, V value,
                                            Supplier<Collection<V>> supplier) {
        assert pkey.cardinality().multiple();
        HugeProperty<Collection<V>> property;
        if (this.hasProperty(pkey.id())) {
            property = this.getProperty(pkey.id());
        } else {
            property = this.newProperty(pkey, supplier.get());
            this.setProperty(property);
        }

        Collection<V> values;
        if (pkey.cardinality() == Cardinality.SET) {
            if (value instanceof Set) {
                values = (Set<V>) value;
            } else {
                values = CollectionUtil.toSet(value);
            }
        } else {
            assert pkey.cardinality() == Cardinality.LIST;
            if (value instanceof List) {
                values = (List<V>) value;
            } else {
                values = CollectionUtil.toList(value);
            }
        }
        property.value().addAll(pkey.validValueOrThrow(values));

        // Any better ways?
        return (HugeProperty) property;
    }

    public void resetProperties() {
        this.properties = CollectionFactory.newIntObjectMap();
        this.propLoaded = false;
    }

    public void copyProperties(HugeElement element) {
        if (element.properties == EMPTY_MAP) {
            this.properties = EMPTY_MAP;
        } else {
            this.properties = CollectionFactory.newIntObjectMap(
                              element.properties);
        }
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
     * @param keyValues The property key-value pair of the vertex or edge
     * @return          Key-value pairs that are classified and processed
     */
    @Watched(prefix = "element")
    public static final ElementKeys classifyKeys(Object... keyValues) {
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

    public static final Id getIdValue(HugeType type, Object idValue) {
        assert type.isGraph();
        Id id = getIdValue(idValue);
        if (type.isVertex()) {
            return id;
        } else {
            if (id == null || id instanceof EdgeId) {
                return id;
            }
            return EdgeId.parse(id.asString());
        }
    }

    @Watched(prefix = "element")
    protected static Id getIdValue(Object idValue) {
        if (idValue == null) {
            return null;
        }

        if (idValue instanceof String) {
            // String id
            return IdGenerator.of((String) idValue);
        } else if (idValue instanceof Number) {
            // Long id
            return IdGenerator.of(((Number) idValue).longValue());
        } else if (idValue instanceof UUID) {
            // UUID id
            return IdGenerator.of((UUID) idValue);
        } else if (idValue instanceof Id) {
            // Id itself
            return (Id) idValue;
        } else if (idValue instanceof Element) {
            // Element
            return (Id) ((Element) idValue).id();
        }

        // Throw if error type
        throw new UnsupportedOperationException(String.format(
                  "Invalid element id: %s(%s)",
                  idValue, idValue.getClass().getSimpleName()));
    }

    @Watched(prefix = "element")
    public static final Object getLabelValue(Object... keyValues) {
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

    public static int intFromId(Id id) {
        E.checkArgument(id instanceof IdGenerator.LongId,
                        "Can't get number from %s(%s)", id, id.getClass());
        return ((IdGenerator.LongId) id).intValue();
    }

    public static final class ElementKeys {

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
