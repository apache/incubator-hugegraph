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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGeneratorFactory;
import com.baidu.hugegraph.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.perf.PerfUtil.Watched;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.type.ExtendableIterator;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.type.define.IdStrategy;
import com.baidu.hugegraph.util.CollectionUtil;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableSet;

public class HugeVertex extends HugeElement implements Vertex, Cloneable {

    protected GraphTransaction tx;
    protected VertexLabel label;
    protected String name;
    protected Set<HugeEdge> edges;

    public HugeVertex(final GraphTransaction tx, Id id, VertexLabel label) {
        this(tx.graph(), id, label);
        this.tx = tx;
        this.fresh = true;
    }

    public HugeVertex(final HugeGraph graph, Id id, VertexLabel label) {
        super(graph, id);
        this.label = label;
        this.edges = new LinkedHashSet<>();
        this.tx = null;
        this.name = null;
    }

    @Override
    public HugeType type() {
        return HugeType.VERTEX;
    }

    @Override
    public String name() {
        assert this.label.idStrategy() == IdStrategy.PRIMARY_KEY;
        if (this.name == null) {
            if (this.id != null) {
                String[] parts = SplicingIdGenerator.parse(this.id);
                E.checkState(parts.length == 2,
                             "Invalid vertex id '%s'", this.id);
                this.name = parts[1];
            } else {
                assert this.id == null;
                List<Object> propValues = primaryValues();
                E.checkState(!propValues.isEmpty(),
                             "Primary values must not be empty " +
                             "(has properties %s)", hasProperties());
                this.name = SplicingIdGenerator.concatValues(propValues);
            }
        }
        return this.name;
    }

    @Override
    protected GraphTransaction tx() {
        GraphTransaction tx = this.tx;
        if (tx == null) {
            tx = super.graph().graphTransaction();
        }
        E.checkNotNull(tx, "transaction");
        return tx;
    }

    public HugeVertex resetTx() {
        this.tx = null;
        return this;
    }

    @Watched(prefix = "vertex")
    public void assignId(Id id) {
        IdStrategy strategy = this.label.idStrategy();
        // Generate an id and assign
        if (strategy == IdStrategy.CUSTOMIZE) {
            this.id = id;
        } else {
            this.id = IdGeneratorFactory.generator(strategy).generate(this);
        }
        assert this.id != null;
    }

    @Override
    public String label() {
        return this.label.name();
    }

    public VertexLabel vertexLabel() {
        return this.label;
    }

    public void vertexLabel(VertexLabel label) {
        this.label = label;
    }

    @Watched(prefix = "vertex")
    public List<Object> primaryValues() {
        E.checkArgument(this.label.idStrategy() == IdStrategy.PRIMARY_KEY,
                        "The id strategy '%s' don't have primary keys",
                        this.label.idStrategy());
        List<String> primaryKeys = this.label.primaryKeys();
        E.checkArgument(!primaryKeys.isEmpty(),
                        "Primary key can't be empty for id strategy '%s'",
                        IdStrategy.PRIMARY_KEY);

        List<Object> propValues = new ArrayList<>(primaryKeys.size());
        for (String pk : primaryKeys) {
            propValues.add(this.property(pk).value());
        }
        return propValues;
    }

    public boolean existsEdges() {
        return this.edges.size() > 0;
    }

    public Set<HugeEdge> getEdges() {
        return Collections.unmodifiableSet(this.edges);
    }

    public void resetEdges() {
        this.edges = new LinkedHashSet<>();
    }

    public void removeEdge(HugeEdge edge) {
        this.edges.remove(edge);
    }

    public void addEdge(HugeEdge edge) {
        this.edges.add(edge);
    }

    @Watched(prefix = "vertex")
    @Override
    public Edge addEdge(String label, Vertex vertex, Object... keyValues) {
        ElementKeys elemKeys = HugeElement.classifyKeys(keyValues);
        // Check id (must be null)
        if (elemKeys.id() != null) {
            throw Edge.Exceptions.userSuppliedIdsNotSupported();
        }
        Id id = HugeElement.getIdValue(elemKeys.id());
        Set<String> keys = elemKeys.keys();

        // Check target vertex
        E.checkArgumentNotNull(vertex, "Target vertex can't be null");
        E.checkArgument(vertex instanceof HugeVertex,
                        "Target vertex must be an instance of HugeVertex");
        HugeVertex targetVertex = (HugeVertex) vertex;

        // Check label
        E.checkArgument(label != null && !label.isEmpty(),
                        "Edge label can't be null or empty");
        EdgeLabel edgeLabel = this.graph().edgeLabel(label);
        // Check link
        E.checkArgument(edgeLabel.checkLinkEqual(this.label(), vertex.label()),
                        "Undefined link of edge label '%s': '%s' -> '%s'",
                        label, this.label(), vertex.label());
        // Check sortKeys
        E.checkArgument(CollectionUtil.containsAll(keys, edgeLabel.sortKeys()),
                        "The sort key(s) must be setted for the edge " +
                        "with label: '%s'", edgeLabel.name());

        // Check weather passed all non-null props
        Collection<?> nonNullKeys = CollectionUtils.subtract(
                                    edgeLabel.properties(),
                                    edgeLabel.nullableKeys());
        if (!keys.containsAll(nonNullKeys)) {
            E.checkArgument(false, "All non-null property keys: %s " +
                            "of edge label '%s' must be setted, " +
                            "but missed keys: %s",
                            nonNullKeys, edgeLabel.name(),
                            CollectionUtils.subtract(nonNullKeys, keys));
        }

        HugeEdge edge = new HugeEdge(this, id, edgeLabel);

        edge.vertices(this, targetVertex);

        // Set properties
        ElementHelper.attachProperties(edge, keyValues);

        // Set id if it not exists
        if (id == null) {
            edge.assignId();
        }

        // Attach edge to vertex
        this.addOutEdge(edge);
        targetVertex.addInEdge(edge.switchOwner());

        return this.tx().addEdge(edge);
    }

    /**
     * Add edge with direction OUT
     */
    public void addOutEdge(HugeEdge edge) {
        if (edge.ownerVertex() == null) {
            edge.ownerVertex(this);
            edge.sourceVertex(this);
        }
        E.checkState(edge.isDirection(Direction.OUT),
                     "The owner vertex('%s') of OUT edge '%s' should be '%s'",
                     edge.ownerVertex().id(), edge, this.id());
        this.edges.add(edge);
    }

    /**
     * Add edge with direction IN
     */
    public void addInEdge(HugeEdge edge) {
        if (edge.ownerVertex() == null) {
            edge.ownerVertex(this);
            edge.targetVertex(this);
        }
        E.checkState(edge.isDirection(Direction.IN),
                     "The owner vertex('%s') of IN edge '%s' should be '%s'",
                     edge.ownerVertex().id(), edge, this.id());
        this.edges.add(edge);
    }

    public Iterator<Edge> getEdges(Direction direction, String... edgeLabels) {
        List<Edge> list = new LinkedList<>();
        for (HugeEdge edge : this.edges) {
            if ((edge.isDirection(direction) || direction == Direction.BOTH)
                && edge.belongToLabels(edgeLabels)) {
                list.add(edge);
            }
        }
        return list.iterator();
    }

    public Iterator<Vertex> getVertices(Direction direction,
                                        String... edgeLabels) {
        List<Vertex> list = new LinkedList<>();
        Iterator<Edge> edges = this.getEdges(direction, edgeLabels);
        while (edges.hasNext()) {
            HugeEdge edge = (HugeEdge) edges.next();
            list.add(edge.otherVertex(this));
        }
        return list.iterator();
    }

    @Watched(prefix = "vertex")
    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        // NOTE: get edges from memory if load all edges when loading vertex.
        if (this.existsEdges()) {
            return this.getEdges(direction, edgeLabels);
        }

        // Deal with direction is BOTH
        ImmutableSet<Direction> directions = ImmutableSet.of(direction);
        if (direction == Direction.BOTH) {
            directions = ImmutableSet.of(Direction.OUT, Direction.IN);
        }

        ExtendableIterator<Edge> results = new ExtendableIterator<>();
        for (Direction dir : directions) {
            Query query = GraphTransaction.constructEdgesQuery(
                          this.id(), dir, edgeLabels);
            results.extend(this.tx().queryEdges(query).iterator());
        }
        return results;
    }

    @Watched(prefix = "vertex")
    @Override
    public Iterator<Vertex> vertices(Direction direction,
                                     String... edgeLabels) {
        Iterator<Edge> edges = this.edges(direction, edgeLabels);
        return this.tx().queryAdjacentVertices(edges).iterator();
    }

    @Watched(prefix = "vertex")
    @Override
    public void remove() {
        this.removed = true;
        this.tx().removeVertex(this);
    }

    @Watched(prefix = "vertex")
    @Override
    @SuppressWarnings("unchecked") // (VertexProperty<V>) prop
    public <V> VertexProperty<V> property(
               VertexProperty.Cardinality cardinality,
               String key, V value, Object... objects) {
        if (objects.length != 0 && objects[0].equals(T.id)) {
            throw VertexProperty.Exceptions.userSuppliedIdsNotSupported();
        }
        // TODO: extra props: objects
        if (objects.length != 0) {
            throw VertexProperty.Exceptions.metaPropertiesNotSupported();
        }

        // Check key in vertex label
        E.checkArgument(this.label.properties().contains(key),
                        "Invalid property '%s' for vertex label '%s', " +
                        "expect: %s", key, label(), vertexLabel().properties());
        // Primary-Keys can only be set once
        if (vertexLabel().primaryKeys().contains(key)) {
            E.checkArgument(!this.hasProperty(key),
                            "Can't update primary key: '%s'", key);
        }
        return (VertexProperty<V>) this.addProperty(key, value, true);
    }

    @Watched(prefix = "vertex")
    @Override
    protected <V> HugeVertexProperty<V> newProperty(PropertyKey pkey, V val) {
        return new HugeVertexProperty<>(this, pkey, val);
    }

    @Watched(prefix = "vertex")
    @Override
    protected <V> void onUpdateProperty(Cardinality cardinality,
                                        HugeProperty<V> prop) {
        if (prop != null) {
            assert prop instanceof HugeVertexProperty;
            // Use addVertexProperty() to update
            this.tx().addVertexProperty((HugeVertexProperty<V>) prop);
        }
    }

    @Watched(prefix = "vertex")
    protected void ensureVertexProperties() {
        if (this.propLoaded) {
            return;
        }

        Iterator<Vertex> vertices = tx().queryVertices(this.id()).iterator();
        assert vertices.hasNext();
        this.copyProperties((HugeVertex) vertices.next());
    }

    @Watched(prefix = "vertex")
    @Override
    @SuppressWarnings("unchecked") // (VertexProperty<V>) prop
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        // TODO: Compatible with TinkerPop properties() (HugeGraph-742)

        this.ensureVertexProperties();

        // Capacity should be about the following size
        int propsCapacity = propertyKeys.length == 0 ?
                            this.sizeOfProperties() :
                            propertyKeys.length;
        List<VertexProperty<V>> props = new ArrayList<>(propsCapacity);

        if (propertyKeys.length == 0) {
            for (HugeProperty<?> prop : this.getProperties().values()) {
                assert prop instanceof VertexProperty;
                props.add((VertexProperty<V>) prop);
            }
        } else {
            for (String pk : propertyKeys) {
                HugeProperty<? extends Object> prop = this.getProperty(pk);
                if (prop == null) {
                    // Not found
                    continue;
                }
                assert prop instanceof VertexProperty;
                props.add((VertexProperty<V>) prop);
            }
        }

        return props.iterator();
    }

    @Override
    public Object sysprop(HugeKeys key) {
        switch (key) {
            case LABEL:
                return this.label();
            case PRIMARY_VALUES:
                return this.name();
            case PROPERTIES:
                return this.getPropertiesMap();
            default:
                E.checkArgument(false,
                                "Invalid system property '%s' of Vertex", key);
                return null;
        }
    }

    /**
     * Clear edges/properties of the cloned vertex, and set `removed` true
     * @return a new vertex
     */
    public HugeVertex prepareRemoved() {
        // NOTE: clear edges/properties of the cloned vertex and return
        HugeVertex vertex = this.clone();
        vertex.removed = true; /* Remove self */
        vertex.resetEdges();
        vertex.resetProperties();
        return vertex;
    }

    @Override
    public HugeVertex copy() {
        HugeVertex vertex = this.clone();
        vertex.properties = new HashMap<>(vertex.properties);
        return vertex;
    }

    @Override
    protected HugeVertex clone() {
        try {
            HugeVertex clone = (HugeVertex) super.clone();
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new HugeException("Failed to clone HugeVertex", e);
        }
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }
}
