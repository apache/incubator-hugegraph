package com.baidu.hugegraph2.structure;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import com.baidu.hugegraph.core.HugeGraph;

/**
 * Created by jishilei on 17/3/16.
 */
public class HugeVertex extends HugeElement implements Vertex {

    public HugeVertex(final HugeGraph graph, final Object id, final String label) {
        super(graph, id, label);
    }

    @Override
    public Edge addEdge(String s, Vertex vertex, Object... objects) {
        return null;
    }

    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String s, V v, Object... objects) {
        return null;
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
