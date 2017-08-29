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
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableList;

public class HugeEdge extends HugeElement implements Edge, Cloneable {

    protected EdgeLabel label;
    protected String name;

    // The Vertex who owned me
    protected HugeVertex owner;
    protected HugeVertex sourceVertex;
    protected HugeVertex targetVertex;

    public HugeEdge(final HugeGraph graph, Id id, EdgeLabel label) {
        super(graph, id);
        this.label = label;
    }

    @Override
    public HugeType type() {
        // NOTE: we optimize the edge type that let it include direction
        return this.owner == this.sourceVertex ?
                             HugeType.EDGE_OUT :
                             HugeType.EDGE_IN;
    }

    @Override
    public GraphTransaction tx() {
        if (this.owner() == null) {
            return null;
        }
        return this.owner().tx();
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

    public EdgeLabel edgeLabel() {
        return this.label;
    }

    public Direction direction() {
        if (!this.label.directed()) {
            return Direction.BOTH;
        }

        if (this.type() == HugeType.EDGE_OUT) {
            return Direction.OUT;
        } else {
            assert this.type() == HugeType.EDGE_IN;
            return Direction.IN;
        }
    }

    public void assignId() {
        assert this.id == null;
        // Generate an id and assign
        if (this.id == null) {
            this.id = SplicingIdGenerator.instance().generate(this);
        }
    }

    public List<Object> sortValues() {
        List<String> sortKeys = this.edgeLabel().sortKeys();
        if (sortKeys.isEmpty()) {
            return ImmutableList.of();
        }

        List<Object> propValues = new ArrayList<>(sortKeys.size());
        for (String sk : sortKeys) {
            propValues.add(this.property(sk).value());
        }
        return propValues;
    }

    @Override
    public void remove() {
        this.removed = true;
        this.sourceVertex.removeEdge(this);
        this.targetVertex.removeEdge(this);
        this.tx().removeEdge(this);
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        E.checkArgument(this.label.properties().contains(key),
                        "Invalid property '%s' for edge label '%s', " +
                        "expected: %s",
                        key, this.label(), this.edgeLabel().properties());
        HugeProperty<V> prop = this.addProperty(key, value);

        /*
         * Edge is new if id is null, Note that, currently we don't support
         * custom id. Maybe the Vertex.attachProperties() has not been called
         * if we support custom id, that should be improved in the future.
         */
        if (prop != null && this.id() != null) {
            assert prop instanceof HugeEdgeProperty;
            this.tx().addEdgeProperty((HugeEdgeProperty<V>) prop);
        }
        return prop;
    }

    @Override
    protected <V> HugeEdgeProperty<V> newProperty(PropertyKey pkey, V val) {
        return new HugeEdgeProperty<>(this, pkey, val);
    }

    @Override
    @SuppressWarnings("unchecked") // (Property<V>) prop
    public <V> Iterator<Property<V>> properties(String... propertyKeys) {
        // Capacity should be about the following size
        int propsCapacity = propertyKeys.length == 0 ?
                            this.sizeOfProperties() :
                            propertyKeys.length;
        List<Property<V>> props = new ArrayList<>(propsCapacity);

        if (propertyKeys.length == 0) {
            for (HugeProperty<?> prop : this.getProperties().values()) {
                assert prop instanceof Property;
                props.add((Property<V>) prop);
            }
        } else {
            for (String pk : propertyKeys) {
                HugeProperty<? extends Object> prop = this.getProperty(pk);
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
                throw new AssertionError("Unsupported direction: " +
                                         direction);
        }

        for (Vertex vertex : vertices) {
            this.ensureVertexProperties((HugeVertex) vertex);
        }
        return vertices.iterator();
    }

    protected void ensureVertexProperties(HugeVertex vertex) {
        if (vertex.hasProperties()) {
            return;
        }

        Iterator<Vertex> vertices = tx().queryVertices(vertex.id()).iterator();
        assert vertices.hasNext();
        HugeVertex fetched = (HugeVertex) vertices.next();
        vertex.setProperties(fetched.getProperties());
    }

    public void vertices(HugeVertex source, HugeVertex target) {
        this.owner = source; // The default owner is the source vertex
        this.sourceVertex = source;
        this.targetVertex = target;

        // TODO: it should be improved that currently we just clone the
        // target to support self-to-self edge.
        // edge from V to V(the vertex itself)
        if (source == target) {
            this.targetVertex = target.clone();
        }
    }

    public HugeEdge switchOwner() {
        HugeEdge edge = this.clone();

        E.checkState(edge.sourceVertex != edge.targetVertex,
                     "Can't switch owner of self-to-self edge");
        if (edge.owner == edge.sourceVertex) {
            edge.owner = edge.targetVertex;
        } else {
            edge.owner = edge.sourceVertex;
        }

        return edge;
    }

    public HugeEdge switchToOutDirection() {
        if (this.type() == HugeType.EDGE_IN) {
            return this.switchOwner();
        }
        return this;
    }

    public HugeVertex owner() {
        return this.owner;
    }

    public void owner(HugeVertex owner) {
        this.owner = owner;
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

    public HugeVertex otherVertex(HugeVertex vertex) {
        if (vertex == this.sourceVertex) {
            return this.targetVertex;
        } else {
            return this.sourceVertex;
        }
    }

    public HugeVertex otherVertex() {
        return this.otherVertex(this.owner);
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

    /**
     * Clear properties of the edge, and set `removed` false
     * @return a new edge
     */
    public HugeEdge prepareRemovedChildren() {
        HugeEdge edge = this.clone();
        edge.removed = false;
        edge.resetProperties();
        return edge;
    }

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
}
