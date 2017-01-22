/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package com.baidu.hugegraph.structure;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import com.baidu.hugegraph.utils.HugeGraphUtils;

/**
 * Created by zhangsuochao on 17/2/6.
 */
public class HugeVertex extends HugeElement implements Vertex {

    public HugeVertex(final HugeGraph graph, final Object id, final String label) {
        super(graph, id, label);
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Object idValue = HugeGraphUtils.generateIdIfNeeded(null);
        HugeEdge edge = new HugeEdge(this.graph(), idValue, label, inVertex, this);
        Long currentTime = System.currentTimeMillis();
        edge.setCreatedAt(currentTime);
        edge.setUpdatedAt(currentTime);
        edge.setProperties(keyValues);
        ((HugeGraph) this.graph()).getEdgeService().addEdge(edge);
        return edge;
    }

    @Override
    public <V> HugeVertexProperty<V> property(final String key) {
        return new HugeVertexProperty<>(this.graph(), this, key, this.getProperty(key));
    }

    @Override
    public <V> HugeVertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value,
                                              Object... keyValues) {
        if (cardinality != VertexProperty.Cardinality.single) {
            throw VertexProperty.Exceptions.multiPropertiesNotSupported();
        }
        if (keyValues.length > 0) {
            throw VertexProperty.Exceptions.metaPropertiesNotSupported();
        }
        this.setProperties(key, value);
        // update property
        ((HugeGraph) this.graph()).vertexService.updateProperty(this, key, value);
        return new HugeVertexProperty<>(this.graph(), this, key, value);
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {

        return ((HugeGraph) this.graph()).edgeService.findVertexEdges(this, direction, edgeLabels);
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        Iterator<Edge> edges = ((HugeGraph) this.graph()).edgeService.findVertexEdges(this, direction, edgeLabels);
        List<Vertex> vertices = new ArrayList<>();
        HugeEdge edge;
        while (edges.hasNext()) {
            edge = (HugeEdge) edges.next();
            vertices.add(edge.getVertex(direction));
        }
        return vertices.iterator();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("HugeVertex.remove() Unimplements yet ");
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        List<VertexProperty<V>> propertyList = new ArrayList<>();
        for (String pk : propertyKeys) {
            HugeVertexProperty hp = new
                    HugeVertexProperty(this.graph(), this, pk, this.getProperties().get(pk));
            propertyList.add(hp);
        }
        return propertyList.iterator();
    }

}
