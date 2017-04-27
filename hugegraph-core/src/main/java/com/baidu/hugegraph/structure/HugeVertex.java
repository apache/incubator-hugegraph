package com.baidu.hugegraph.structure;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGeneratorFactory;
import com.baidu.hugegraph.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph.schema.HugeEdgeLabel;
import com.baidu.hugegraph.type.HugeTypes;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.schema.EdgeLabel;
import com.baidu.hugegraph.type.schema.PropertyKey;
import com.baidu.hugegraph.type.schema.VertexLabel;
import com.baidu.hugegraph.util.CollectionUtil;
import com.google.common.base.Preconditions;

public class HugeVertex extends HugeElement implements Vertex {

    protected VertexLabel label;
    protected Set<HugeEdge> edges;

    public HugeVertex(final HugeGraph graph, final Id id, final VertexLabel label) {
        super(graph, id);
        this.label = label;
        this.edges = new LinkedHashSet<>();
    }

    @Override
    public HugeTypes type() {
        return HugeTypes.VERTEX;
    }

    @Override
    public String name() {
        List<String> properties = new LinkedList<>();
        for (String key : this.vertexLabel().primaryKeys()) {
            properties.add(this.property(key).value().toString());
        }
        assert !properties.isEmpty() : "Primary values must not be empty";
        // TODO: use a better delimiter
        return String.join(SplicingIdGenerator.NAME_SPLITOR, properties);
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

    public Set<HugeEdge> getEdges() {
        // TODO: return a list of HugeEdge
        return this.edges;
    }

    @Override
    public Edge addEdge(String label, Vertex vertex, Object... properties) {
        HugeVertex targetVertex = (HugeVertex) vertex;
        EdgeLabel edgeLabel = this.graph.schema().edgeLabel(label);

        Preconditions.checkArgument(
                CollectionUtil.containsAll(ElementHelper.getKeys(properties), ((HugeEdgeLabel) edgeLabel)
                        .sortKeys()), "the sort key must "
                        + "set in 'addEdge' method, you can refer to the definition of edgeLabel.");

        Id id = HugeElement.getIdValue(properties);

        HugeEdge edge = new HugeEdge(this.graph, id, edgeLabel);

        edge.vertices(this, targetVertex);
        edge = this.addOutEdge(edge) ? edge : null;

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

        return edge;
    }

    // add edge of direction OUT
    public boolean addOutEdge(HugeEdge edge) {
        if (edge.owner() == null) {
            edge.owner(this);
            edge.sourceVertex(this);
        }
        assert edge.type() == HugeTypes.EDGE_OUT;
        return this.edges.add(edge);
    }

    // add edge of direction IN
    public boolean addInEdge(HugeEdge edge) {
        if (edge.owner() == null) {
            edge.owner(this);
            edge.targetVertex(this);
        }
        assert edge.type() == HugeTypes.EDGE_IN;
        return this.edges.add(edge);
    }

    public boolean edge(HugeEdge edge) {
        return this.edges.add(edge);
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        List<Edge> list = new LinkedList<>();
        for (HugeEdge edge : this.edges) {
            if ((edge.direction() == direction
                    || direction == Direction.BOTH)
                    && edge.belongToLabels(edgeLabels)) {
                list.add(edge);
            }
        }
        return list.iterator();
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        List<Vertex> list = new LinkedList<>();
        Iterator<Edge> edges = this.edges(direction, edgeLabels);
        while (edges.hasNext()) {
            HugeEdge edge = (HugeEdge) edges.next();
            list.add(edge.otherVertex(this));
        }
        return list.iterator();
    }

    @Override
    public void remove() {
        throw Vertex.Exceptions.vertexRemovalNotSupported();
    }

    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality,
                                          String key, V value, Object... objects) {
        // TODO: extra props
        PropertyKey pkey = this.graph.schema().propertyKey(key);
        switch (Cardinality.convert(cardinality)) {
            case SINGLE:
                HugeVertexProperty<V> prop = new HugeVertexProperty<V>(this, pkey, value);
                return super.setProperty(prop) != null ? prop : null;
            case SET:
                Preconditions.checkArgument(pkey.checkDataType(value), String.format(
                        "Invalid property value '%s' for key '%s'", value, key));

                HugeVertexProperty<Set<V>> propSet;
                if (!super.existsProperty(key)) {
                    propSet = new HugeVertexProperty<>(this, pkey, new LinkedHashSet<V>());
                    super.setProperty(propSet);
                }
                else {
                    propSet = (HugeVertexProperty<Set<V>>) super.getProperty(key);
                }

                propSet.value().add(value);

                // any better ways?
                return (VertexProperty<V>) propSet;
            case LIST:
                Preconditions.checkArgument(pkey.checkDataType(value), String.format(
                        "Invalid property value '%s' for key '%s'", value, key));

                HugeVertexProperty<List<V>> propList;
                if (!super.existsProperty(key)) {
                    propList = new HugeVertexProperty<>(this, pkey, new LinkedList<V>());
                    super.setProperty(propList);
                }
                else {
                    propList = (HugeVertexProperty<List<V>>) super.getProperty(key);
                }

                propList.value().add(value);

                // any better ways?
                return (VertexProperty<V>) propList;
            default:
                assert false;
                break;
        }
        return null;
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        List<VertexProperty<V>> propertyList = new ArrayList<>(propertyKeys.length);

        if (propertyKeys.length == 0) {
            for (HugeProperty<?> prop : this.getProperties().values()) {
                propertyList.add((VertexProperty<V>) prop);
            }
        } else {
            for (String pk : propertyKeys) {
                HugeProperty<? extends Object> prop = this.getProperty(pk);
                assert prop instanceof VertexProperty;
                propertyList.add((VertexProperty<V>) prop);
            }
        }

        return propertyList.iterator();
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
