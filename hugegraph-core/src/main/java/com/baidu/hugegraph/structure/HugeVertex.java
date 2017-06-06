package com.baidu.hugegraph.structure;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGeneratorFactory;
import com.baidu.hugegraph.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.schema.HugeEdgeLabel;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.schema.PropertyKey;
import com.baidu.hugegraph.type.schema.VertexLabel;
import com.baidu.hugegraph.util.CollectionUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

public class HugeVertex extends HugeElement implements Vertex, Cloneable {

    protected GraphTransaction tx;
    protected VertexLabel label;
    protected String name;
    protected Set<HugeEdge> edges;

    public HugeVertex(final GraphTransaction tx, Id id, VertexLabel label) {
        super(tx.graph(), id);
        this.tx = tx;
        this.label = label;
        this.edges = new LinkedHashSet<>();
    }

    @Override
    public HugeType type() {
        return HugeType.VERTEX;
    }

    @Override
    public String name() {
        if (this.name == null) {
            List<Object> propValues = primaryValues();
            assert !propValues.isEmpty() : "Primary values must not be empty";
            this.name = SplicingIdGenerator.concatValues(propValues);
        }
        return this.name;
    }

    public void name(String name) {
        this.name = name;
    }

    @Override
    public GraphTransaction tx() {
        Preconditions.checkNotNull(this.tx);
        return this.tx;
    }

    public void assignId() {
        assert this.id == null;
        // generate an id and assign
        if (this.id == null) {
            this.id = IdGeneratorFactory.generator().generate(this);
        }
    }

    @Override
    public String label() {
        return this.label.name();
    }

    public VertexLabel vertexLabel() {
        return this.label;
    }

    public List<Object> primaryValues() {
        Set<String> primaryKeys = this.vertexLabel().primaryKeys();
        if (primaryKeys.isEmpty()) {
            return ImmutableList.of();
        }
        Iterator<VertexProperty<Object>> props = this.properties(
                primaryKeys.toArray(new String[0]));

        List<Object> propValues = new ArrayList<>(primaryKeys.size());
        while (props.hasNext()) {
            propValues.add(props.next().value());
        }
        return propValues;
    }

    public void primaryValues(List<Object> propValues) {
        Set<String> primaryKeys = this.vertexLabel().primaryKeys();
        int i = 0;
        for (String k : primaryKeys) {
            this.addProperty(k, propValues.get(i++));
        }
    }

    public boolean existsEdges() {
        return this.edges.size() > 0;
    }

    public Set<HugeEdge> getEdges() {
        return this.edges;
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

    @Override
    public Edge addEdge(String label, Vertex vertex, Object... properties) {
        Preconditions.checkNotNull(label,
                "The edge label can not be null.");
        Preconditions.checkNotNull(vertex,
                "The target vertex can not be null.");

        HugeVertex targetVertex = (HugeVertex) vertex;
        HugeEdgeLabel edgeLabel =
                (HugeEdgeLabel) this.graph.schema().edgeLabel(label);

        Preconditions.checkArgument(CollectionUtil.containsAll(
                ElementHelper.getKeys(properties), edgeLabel.sortKeys()),
                "The sort key(s) must be setted for the edge with label: '%s'",
                edgeLabel.name());

        Preconditions.checkArgument(
                edgeLabel.checkLink(this.label(), vertex.label()),
                String.format("Undefined link of edge label '%s': '%s' -> '%s'",
                        label, this.label(), vertex.label()));

        Id id = HugeElement.getIdValue(properties);

        HugeEdge edge = new HugeEdge(this.graph, id, edgeLabel);

        edge.vertices(this, targetVertex);
        this.addOutEdge(edge);

        // set properties
        ElementHelper.attachProperties(edge, properties);

        // set id if it not exists
        if (id == null) {
            edge.assignId();
        }

        // add to other Vertex
        if (edge != null) {
            targetVertex.addInEdge(edge.switchOwner());
        }

        return this.tx().addEdge(edge);
    }

    // add edge with direction OUT
    public void addOutEdge(HugeEdge edge) {
        if (edge.owner() == null) {
            edge.owner(this);
            edge.sourceVertex(this);
        }
        Preconditions.checkState(edge.type() == HugeType.EDGE_OUT,
                "The owner vertex('%s') of OUT edge '%s' should be '%s'",
                edge.owner().id, edge, this.id());
        this.edges.add(edge);
    }

    // add edge with direction IN
    public void addInEdge(HugeEdge edge) {
        if (edge.owner() == null) {
            edge.owner(this);
            edge.targetVertex(this);
        }
        Preconditions.checkState(edge.type() == HugeType.EDGE_IN,
                "The owner vertex('%s') of IN edge '%s' should be '%s'",
                edge.owner().id(), edge, this.id());
        this.edges.add(edge);
    }

    public Iterator<Edge> getEdges(Direction direction, String... edgeLabels) {
        List<Edge> list = new LinkedList<>();
        for (HugeEdge edge : this.edges) {
            if ((edge.direction() == direction || direction == Direction.BOTH)
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

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        // NOTE: get edges from memory if load all edges when loading vertex
        if (this.existsEdges()) {
            return this.getEdges(direction, edgeLabels);
        }

        Query query = GraphTransaction.constructEdgesQuery(
                this.id, direction, edgeLabels);
        return this.tx().queryEdges(query);
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        Iterator<Edge> edges = this.edges(direction, edgeLabels);
        return this.tx().queryAdjacentVertices(edges);
    }

    @Override
    public void remove() {
        this.removed = true;
        this.tx().removeVertex(this);
    }

    @Override
    @SuppressWarnings("unchecked") // (VertexProperty<V>) prop
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality,
            String key, V value, Object... objects) {
        if (objects.length != 0 && objects[0].equals(T.id)) {
            throw VertexProperty.Exceptions.userSuppliedIdsNotSupported();
        }
        // TODO: extra props: objects
        if (objects.length != 0) {
            throw VertexProperty.Exceptions.metaPropertiesNotSupported();
        }

        Preconditions.checkArgument(this.label.properties().contains(key),
                "Invalid property '%s' for Vertex '%s', "
                + "valid properties are '%s'",
                key, this.label(), this.label.properties());

        HugeProperty<V> prop = this.addProperty(key, value);

        // NOTE: currently we don't support custom id, if id is null
        // maybe the Vertex.attachProperties() has not been called.
        // if we support custom id, this should be improved!
        if (prop != null && this.id() != null) {
            assert prop instanceof VertexProperty;
            // update self (TODO: add tx.addProperty() method)
            this.tx().addVertex(this);
        }
        return (VertexProperty<V>) prop;
    }

    @Override
    protected <V> HugeProperty<V> newProperty(PropertyKey pkey, V value) {
        return new HugeVertexProperty<>(this, pkey, value);
    }

    @Override
    @SuppressWarnings("unchecked") // (VertexProperty<V>) prop
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        List<VertexProperty<V>> propertyList = new ArrayList<>(propertyKeys.length);

        if (propertyKeys.length == 0) {
            for (HugeProperty<?> prop : this.getProperties().values()) {
                propertyList.add((VertexProperty<V>) prop);
            }
        } else {
            for (String pk : propertyKeys) {
                HugeProperty<? extends Object> prop = this.getProperty(pk);
                if (prop != null) {
                    assert prop instanceof VertexProperty;
                    propertyList.add((VertexProperty<V>) prop);
                } // else not found
            }
        }

        return propertyList.iterator();
    }

    // TODO: after separating edges from vertex,
    // we will remove these prepareXX() methods
    public HugeVertex prepareAdded() {
        // NOTE: keep properties(without edges) of the cloned vertex and return
        HugeVertex vertex = this.clone();
        vertex.resetEdges();
        return vertex;
    }

    public HugeVertex prepareRemoved() {
        // NOTE: clear edges/properties of the cloned vertex and return
        HugeVertex vertex = this.clone();
        vertex.removed = true; // remove self
        vertex.resetEdges();
        vertex.resetProperties();
        return vertex;
    }

    public HugeVertex prepareRemovedChildren() {
        // NOTE: clear edges/properties of the cloned vertex and return
        HugeVertex vertex = this.clone();
        vertex.removed = false; // not remove self
        vertex.resetEdges();
        vertex.resetProperties();
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
        return String.format("{id=%s, label=%s, edges=%s, properties=%s}",
                this.id,
                this.label.name(),
                this.edges,
                this.properties.values());
    }
}
