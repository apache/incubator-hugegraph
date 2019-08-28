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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.EdgeId;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph.backend.serializer.BytesBuffer;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.perf.PerfUtil.Watched;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableList;

public class HugeEdge extends HugeElement implements Edge, Cloneable {

    protected EdgeLabel label;
    protected String name;

    protected HugeVertex sourceVertex;
    protected HugeVertex targetVertex;
    protected boolean isOutEdge;

    public HugeEdge(HugeVertex sourceVertex, Id id, EdgeLabel label,
                    HugeVertex targetVertex) {
        this(sourceVertex.graph(), id, label);
        this.sourceVertex = sourceVertex;
        this.targetVertex = targetVertex;
        this.isOutEdge = true;
        this.fresh = true;
    }

    public HugeEdge(final HugeGraph graph, Id id, EdgeLabel label) {
        super(graph, id);

        E.checkArgumentNotNull(label, "Edge label can't be null");
        this.label = label;

        this.name = null;
        this.sourceVertex = null;
        this.targetVertex = null;
        this.isOutEdge = true;
    }

    @Override
    public HugeType type() {
        // NOTE: we optimize the edge type that let it include direction
        return this.isOutEdge ? HugeType.EDGE_OUT : HugeType.EDGE_IN;
    }

    @Override
    public EdgeId id() {
        return (EdgeId) this.id;
    }

    @Override
    public EdgeLabel schemaLabel() {
        return this.label;
    }

    @Override
    public GraphTransaction tx() {
        if (this.ownerVertex() == null) {
            return null;
        }
        return this.ownerVertex().tx();
    }

    @Override
    public String name() {
        if (this.name == null) {
            this.name = SplicingIdGenerator.concatValues(sortValues());
        }
        return this.name;
    }

    public void name(String name) {
        this.name = name;
    }

    @Override
    public String label() {
        return this.label.name();
    }

    public boolean selfLoop() {
        return this.sourceVertex != null &&
               this.sourceVertex == this.targetVertex;
    }

    public Directions direction() {
        return this.isOutEdge ? Directions.OUT : Directions.IN;
    }

    public boolean matchDirection(Directions direction) {
        if (direction == Directions.BOTH || this.selfLoop()) {
            return true;
        }
        return this.isDirection(direction);
    }

    public boolean isDirection(Directions direction) {
        return this.isOutEdge && direction == Directions.OUT ||
               !this.isOutEdge && direction == Directions.IN;
    }

    @Watched(prefix = "edge")
    public void assignId() {
        // Generate an id and assign
        this.id = new EdgeId(this.ownerVertex(), this.direction(),
                             this.schemaLabel().id(), this.name(),
                             this.otherVertex());

        int len = this.id.length();
        E.checkArgument(len <= BytesBuffer.BIG_ID_LEN_MAX,
                        "The max length of edge id is %s, but got %s {%s}",
                        BytesBuffer.BIG_ID_LEN_MAX, len, this.id);
    }

    @Watched(prefix = "edge")
    public EdgeId idWithDirection() {
        return ((EdgeId) this.id).directed(true);
    }

    @Watched(prefix = "edge")
    public List<Object> sortValues() {
        List<Id> sortKeys = this.schemaLabel().sortKeys();
        if (sortKeys.isEmpty()) {
            return ImmutableList.of();
        }
        List<Object> propValues = new ArrayList<>(sortKeys.size());
        for (Id sk : sortKeys) {
            HugeProperty<?> property = this.getProperty(sk);
            E.checkState(property != null,
                         "The value of sort key '%s' can't be null", sk);
            propValues.add(property.serialValue());
        }
        return propValues;
    }

    @Watched(prefix = "edge")
    @Override
    public void remove() {
        this.removed = true;
        this.sourceVertex.removeEdge(this);
        this.targetVertex.removeEdge(this);
        this.tx().removeEdge(this);
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        PropertyKey propertyKey = this.graph().propertyKey(key);
        // Check key in edge label
        E.checkArgument(this.label.properties().contains(propertyKey.id()),
                        "Invalid property '%s' for edge label '%s'",
                        key, this.label());
        // Sort-Keys can only be set once
        if (this.schemaLabel().sortKeys().contains(propertyKey.id())) {
            E.checkArgument(!this.hasProperty(propertyKey.id()),
                            "Can't update sort key: '%s'", key);
        }
        return this.addProperty(propertyKey, value, true);
    }

    @Watched(prefix = "edge")
    @Override
    protected <V> HugeEdgeProperty<V> newProperty(PropertyKey pkey, V val) {
        return new HugeEdgeProperty<>(this, pkey, val);
    }

    @Watched(prefix = "edge")
    @Override
    protected <V> void onUpdateProperty(Cardinality cardinality,
                                        HugeProperty<V> prop) {
        if (prop != null) {
            assert prop instanceof HugeEdgeProperty;
            // Use tx to update property (should update cache even if it's new)
            this.tx().addEdgeProperty((HugeEdgeProperty<V>) prop);
        }
    }

    @Watched(prefix = "edge")
    @Override
    protected boolean ensureFilledProperties(boolean throwIfNotExist) {
        if (this.propLoaded) {
            return true;
        }

        Iterator<Edge> edges = tx().queryEdges(this.id());
        boolean exist = edges.hasNext();
        if (!exist && !throwIfNotExist) {
            return false;
        }
        E.checkState(exist, "Edge '%s' does not exist", this.id);
        this.copyProperties((HugeEdge) edges.next());
        assert exist;
        return true;
    }

    @Watched(prefix = "edge")
    @SuppressWarnings("unchecked") // (Property<V>) prop
    @Override
    public <V> Iterator<Property<V>> properties(String... keys) {
        this.ensureFilledProperties(true);

        // Capacity should be about the following size
        int propsCapacity = keys.length == 0 ?
                            this.sizeOfProperties() :
                            keys.length;
        List<Property<V>> props = new ArrayList<>(propsCapacity);

        if (keys.length == 0) {
            for (HugeProperty<?> prop : this.getProperties().values()) {
                assert prop instanceof Property;
                props.add((Property<V>) prop);
            }
        } else {
            for (String key : keys) {
                PropertyKey propertyKey = this.graph().schemaTransaction()
                                              .getPropertyKey(key);
                if (propertyKey == null) {
                    continue;
                }
                HugeProperty<?> prop = this.getProperty(propertyKey.id());
                if (prop == null) {
                    // Not found
                    continue;
                }
                assert prop instanceof Property;
                props.add((Property<V>) prop);
            }
        }
        return props.iterator();
    }

    @Override
    public Object sysprop(HugeKeys key) {
        switch (key) {
            case ID:
                return this.id();
            case OWNER_VERTEX:
                return this.ownerVertex().id();
            case LABEL:
                return this.schemaLabel().id();
            case DIRECTION:
                return this.direction();
            case OTHER_VERTEX:
                return this.otherVertex().id();
            case SORT_VALUES:
                return this.name();
            case PROPERTIES:
                return this.getPropertiesMap();
            default:
                E.checkArgument(false,
                                "Invalid system property '%s' of Edge", key);
                return null;
        }
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        List<Vertex> vertices = new ArrayList<>(2);
        switch (direction) {
            case OUT:
                vertices.add(this.sourceVertex);
                break;
            case IN:
                vertices.add(this.targetVertex);
                break;
            case BOTH:
                vertices.add(this.sourceVertex);
                vertices.add(this.targetVertex);
                break;
            default:
                throw new AssertionError("Unsupported direction: " + direction);
        }

        return vertices.iterator();
    }

    @Override
    public Vertex outVertex() {
        return this.sourceVertex;
    }

    @Override
    public Vertex inVertex() {
        return this.targetVertex;
    }

    public void vertices(boolean isOutEdge,
                         HugeVertex owner,
                         HugeVertex other) {
        this.isOutEdge = isOutEdge;
        if (isOutEdge) {
            this.sourceVertex = owner;
            this.targetVertex = other;
        } else {
            this.sourceVertex = other;
            this.targetVertex = owner;
        }
    }

    public HugeEdge switchOwner() {
        HugeEdge edge = this.clone();
        edge.isOutEdge = !edge.isOutEdge;
        edge.assignId();
        return edge;
    }

    public HugeEdge switchToOutDirection() {
        if (this.direction() == Directions.IN) {
            return this.switchOwner();
        }
        return this;
    }

    public HugeVertex ownerVertex() {
        return this.isOutEdge ? this.sourceVertex : this.targetVertex;
    }

    public HugeVertex sourceVertex() {
        return this.sourceVertex;
    }

    public void sourceVertex(HugeVertex sourceVertex) {
        this.sourceVertex = sourceVertex;
    }

    public HugeVertex targetVertex() {
        return this.targetVertex;
    }

    public void targetVertex(HugeVertex targetVertex) {
        this.targetVertex = targetVertex;
    }

    public boolean belongToLabels(String... edgeLabels) {
        if (edgeLabels.length == 0) {
            return true;
        }

        // Does edgeLabels contain me
        for (String label : edgeLabels) {
            if (label.equals(this.label())) {
                return true;
            }
        }
        return false;
    }

    public boolean belongToVertex(HugeVertex vertex) {
        return vertex != null && (vertex.equals(this.sourceVertex) ||
                                  vertex.equals(this.targetVertex));
    }

    public HugeVertex otherVertex(HugeVertex vertex) {
        if (vertex == this.sourceVertex) {
            return this.targetVertex;
        } else {
            E.checkArgument(vertex == this.targetVertex,
                            "Invalid argument vertex '%s', must be in [%s, %s]",
                            vertex, this.sourceVertex, this.targetVertex);
            return this.sourceVertex;
        }
    }

    public HugeVertex otherVertex() {
        return this.isOutEdge ? this.targetVertex : this.sourceVertex;
    }

    /**
     * Clear properties of the edge, and set `removed` true
     * @return a new edge
     */
    public HugeEdge prepareRemoved() {
        HugeEdge edge = this.clone();
        edge.removed = true;
        edge.resetProperties();
        return edge;
    }

    @Override
    public HugeEdge copy() {
        HugeEdge edge = this.clone();
        edge.properties = new HashMap<>(edge.properties);
        return edge;
    }

    @Override
    protected HugeEdge clone() {
        try {
            return (HugeEdge) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new HugeException("Failed to clone HugeEdge", e);
        }
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

    public static final Id getIdValue(Object idValue) {
        Id id = HugeElement.getIdValue(idValue);
        if (id == null || id instanceof EdgeId) {
            return id;
        }
        return EdgeId.parse(id.asString());
    }
}
