/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package com.baidu.hugegraph.structure;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.utils.HugeGraphUtils;

/**
 * Created by zhangsuochao on 17/2/9.
 */
public class HugeEdge extends HugeElement implements Edge {

    private static final Logger logger = LoggerFactory.getLogger(HugeEdge.class);

    private Vertex inVertex;
    private Vertex outVertex;

    public HugeEdge(final HugeGraph graph, final Object id, final String label) {
        super(graph, id, label);

    }

    public HugeEdge(final Graph graph, final Object id, final String label, final Vertex inVertex, final
        Vertex outVertex) {
        super(graph, id, label);
        this.inVertex = inVertex;
        this.outVertex = outVertex;
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        return direction == Direction.BOTH
                ? IteratorUtils.of(getVertex(Direction.OUT), getVertex(Direction.IN))
                : IteratorUtils.of(getVertex(direction));
    }

    @Override
    public <V> HugeProperty<V> property(final String key) {
        return new HugeProperty<>(this.graph(), this, key, this.getProperty(key));
    }

    @Override
    public <V> HugeProperty<V> property(String key, V value) {
        this.setProperties(key, value);
        ((HugeGraph) this.graph()).edgeService.updateProperty(this, key, value);
        return new HugeProperty<>(this.graph(), this, key, value);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("HugeEdge.remove() Unimplements yet ");
    }

    @Override
    public Iterator<Property> properties(String... propertyKeys) {
        List<Property> propertyList = new ArrayList<>();
        for (String pk : propertyKeys) {
            HugeProperty p = new HugeProperty(this.graph(), this, pk, this.getProperties().get(pk));
            propertyList.add(p);
        }
        return propertyList.iterator();
    }

    public void setProperties(Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        this.properties = HugeGraphUtils.propertiesToMap(keyValues);
    }

    public Vertex getVertex(Direction direction) throws IllegalArgumentException {
        if (!Direction.IN.equals(direction) && !Direction.OUT.equals(direction)) {
            throw new IllegalArgumentException("Invalid direction: " + direction);
        }

        return Direction.IN.equals(direction) ? inVertex : outVertex;
    }

    public void setInVertex(Vertex inVertex) {
        this.inVertex = inVertex;
    }

    public void setOutVertex(Vertex outVertex) {
        this.outVertex = outVertex;
    }
}
