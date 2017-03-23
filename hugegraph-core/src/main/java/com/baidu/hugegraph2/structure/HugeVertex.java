package com.baidu.hugegraph2.structure;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import com.baidu.hugegraph2.backend.id.Id;
import com.baidu.hugegraph2.type.HugeTypes;
import com.baidu.hugegraph2.type.schema.VertexLabel;

/**
 * Created by jishilei on 17/3/16.
 */
public class HugeVertex extends HugeElement implements Vertex {

    public static class HugeVertexProperty<V> extends HugeProperty<V>
            implements VertexProperty<V> {

        public HugeVertexProperty(HugeElement owner, String key, V value) {
            super(owner, key, value);
        }

        @Override
        public HugeTypes type() {
            return HugeTypes.VERTEX_PROPERTY;
        }

        @Override
        public Object id() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public <V> Property<V> property(String key, V value) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Vertex element() {
            return (Vertex) super.element();
        }

        @Override
        public <U> Iterator<Property<U>> properties(String... propertyKeys) {
            return null;
        }

    }

    public HugeVertex(final Graph graph, final Id id, final VertexLabel label) {
        super(graph, id, label);
    }

    @Override
    public HugeTypes type() {
        return HugeTypes.VERTEX;
    }

    public List<Edge> getEdges() {
        // TODO: return a list of HugeEdge
        return new ArrayList<>();
    }

    @Override
    public Edge addEdge(String s, Vertex vertex, Object... objects) {
        return null;
    }

    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality,
            String key, V value, Object... objects) {
        // TODO: extra props
        HugeVertexProperty<V> prop = new HugeVertexProperty<V>(this, key, value);
        return super.setProperty(prop) != null ? prop : null;
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... strings) {
        return null;
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... strings) {
        return null;
    }

    @Override
    public void remove() {
        throw Vertex.Exceptions.vertexRemovalNotSupported();
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        List<VertexProperty<V>> propertyList = new ArrayList<>();
        for (String pk : propertyKeys) {
            // TODO : build props
        }
        return propertyList.iterator();
    }
}
