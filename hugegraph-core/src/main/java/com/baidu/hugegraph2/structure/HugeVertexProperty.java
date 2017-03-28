package com.baidu.hugegraph2.structure;

import java.util.Iterator;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import com.baidu.hugegraph2.type.HugeTypes;

public class HugeVertexProperty<V> extends HugeProperty<V> implements VertexProperty<V> {

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
