package com.baidu.hugegraph.structure;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGeneratorFactory;
import com.baidu.hugegraph.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.type.HugeTypes;
import com.baidu.hugegraph.type.schema.EdgeLabel;
import com.baidu.hugegraph.type.schema.PropertyKey;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

public class HugeEdge extends HugeElement implements Edge, Cloneable {

    protected EdgeLabel label;
    protected String name;

    protected HugeVertex owner; // the Vertex who owned me
    protected HugeVertex sourceVertex;
    protected HugeVertex targetVertex;

    public HugeEdge(final HugeGraph graph, Id id, EdgeLabel label) {
        super(graph, id);
        this.label = label;
    }

    @Override
    public HugeTypes type() {
        // NOTE: we optimize the edge type that let it include direction
        return this.owner == this.sourceVertex ? HugeTypes.EDGE_OUT : HugeTypes.EDGE_IN;
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
        if (!this.label.isDirected()) {
            return Direction.BOTH;
        }

        if (this.type() == HugeTypes.EDGE_OUT) {
            return Direction.OUT;
        }
        else {
            return Direction.IN;
        }
    }

    public void assignId() {
        assert this.id == null;
        // generate an id and assign
        if (this.id == null) {
            this.id = IdGeneratorFactory.generator().generate(this);
        }
    }

    public List<Object> sortValues() {
        Set<String> sortKeys = this.edgeLabel().sortKeys();
        if (sortKeys.isEmpty()) {
            return ImmutableList.of();
        }
        Iterator<Property<Object>> props = this.properties(
                sortKeys.toArray(new String[0]));

        List<Object> propValues = new ArrayList<>(sortKeys.size());
        while (props.hasNext()) {
            propValues.add(props.next().value());
        }
        return propValues;
    }

    public void sortValues(List<Object> propValues) {
        Set<String> sortKeys = this.edgeLabel().sortKeys();
        int i = 0;
        for (String k : sortKeys) {
            this.property(k, propValues.get(i++));
        }
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        HugeProperty<V> prop = null;
        PropertyKey pkey = this.graph.schema().propertyKey(key);
        switch (pkey.cardinality()) {
            case SINGLE:
                prop = new HugeProperty<V>(this, pkey, value);
                super.setProperty(prop);
                break;
            case SET:
                Preconditions.checkArgument(pkey.checkDataType(value), String.format(
                        "Invalid property value '%s' for key '%s'", value, key));

                HugeProperty<Set<V>> propSet;
                if (!super.existsProperty(key)) {
                    propSet = new HugeProperty<>(this, pkey, new LinkedHashSet<V>());
                    super.setProperty(propSet);
                } else {
                    propSet = (HugeProperty<Set<V>>) super.getProperty(key);
                }

                propSet.value().add(value);

                // any better ways?
                prop = (HugeProperty) propSet;
                break;
            case LIST:
                Preconditions.checkArgument(pkey.checkDataType(value), String.format(
                        "Invalid property value '%s' for key '%s'", value, key));

                HugeProperty<List<V>> propList;
                if (!super.existsProperty(key)) {
                    propList = new HugeProperty<>(this, pkey, new LinkedList<V>());
                    super.setProperty(propList);
                } else {
                    propList = (HugeProperty<List<V>>) super.getProperty(key);
                }

                propList.value().add(value);

                // any better ways?
                prop = (HugeProperty) propList;
                break;
            default:
                assert false;
                break;
        }
        return prop;
    }

    @Override
    public void remove() {
        this.removed = true;
        this.sourceVertex.removeEdge(this);
        this.targetVertex.removeEdge(this);
        this.tx().removeEdge(this);
    }

    @Override
    public <V> Iterator<Property<V>> properties(String... propertyKeys) {
        List<Property<V>> propertyList = new ArrayList<>(propertyKeys.length);

        if (propertyKeys.length == 0) {
            for (HugeProperty<?> prop : this.getProperties().values()) {
                propertyList.add((Property<V>) prop);
            }
        } else {
            for (String pk : propertyKeys) {
                HugeProperty<? extends Object> prop = this.getProperty(pk);
                if (prop != null) {
                    assert prop instanceof Property;
                    propertyList.add((Property<V>) prop);
                }
            }
        }
        return propertyList.iterator();
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
                break;
        }
        return vertices.iterator();
    }

    public void vertices(HugeVertex source, HugeVertex target) {
        this.owner = source; // The default owner is the source vertex
        this.sourceVertex = source;
        this.targetVertex = target;
    }

    public HugeEdge switchOwner() {
        HugeEdge edge = this.clone();

        if (edge.owner == edge.sourceVertex) {
            edge.owner = edge.targetVertex;
        } else {
            edge.owner = edge.sourceVertex;
        }

        return edge;
    }

    public HugeEdge switchOutDirection() {
        if (this.type() == HugeTypes.EDGE_IN) {
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

        // does edgeLabels contain me
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
        }
        else {
            return this.sourceVertex;
        }
    }

    public HugeVertex otherVertex() {
        return this.otherVertex(this.owner);
    }

    public HugeVertex prepareAddedOut() {
        // return a vertex just with this edge(OUT)
        HugeVertex sourceVertex = this.sourceVertex.prepareRemoved();
        HugeEdge edge = this.clone();
        edge.vertices(sourceVertex, this.targetVertex);
        edge.owner(sourceVertex);
        sourceVertex.addOutEdge(edge);
        return sourceVertex;
    }

    public HugeEdge prepareAddedOut(HugeVertex sourceVertex) {
        // return a new edge, and add it to sourceVertex as OUT edge
        HugeEdge edge = this.clone();
        edge.vertices(sourceVertex, this.targetVertex);
        edge.owner(sourceVertex);
        return edge;
    }

    public HugeVertex prepareAddedIn() {
        // return a vertex just with this edge(IN)
        HugeVertex targetVertex = this.targetVertex.prepareRemoved();
        HugeEdge edge = this.clone();
        edge.vertices(this.sourceVertex, targetVertex);
        edge.owner(targetVertex);
        targetVertex.addInEdge(edge);
        return targetVertex;
    }

    public HugeEdge prepareAddedIn(HugeVertex targetVertex) {
        // return a new edge, and add it to targetVertex as IN edge
        HugeEdge edge = this.clone();
        edge.vertices(this.sourceVertex, targetVertex);
        edge.owner(targetVertex);
        return edge;
    }

    public HugeEdge prepareRemoved() {
        // NOTE: clear properties of the edge(keep sort-values)
        HugeEdge edge = this.clone();
        edge.resetProperties();
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
        return String.format("{id=%s, label=%s,"
                + " source=%s, target=%s, direction=%s, properties=%s}",
                this.id,
                this.label.name(),
                this.sourceVertex.id(),
                this.targetVertex.id(),
                this.direction().name(),
                this.properties.values());
    }
}
