package com.baidu.hugegraph2.structure;

import java.util.NoSuchElementException;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;

import com.baidu.hugegraph2.type.HugeTypes;

/**
 * Created by jishilei on 17/3/16.
 */
public class HugeProperty<V> implements Property<V>, GraphType {

    protected final HugeElement owner;
    // TODO: change key into PropertyKey
    protected final String key;
    protected final V value;

    public HugeProperty(final HugeElement owner, final String key, final V value) {
        this.owner = owner;
        this.key = key;
        this.value = value;
    }

    @Override
    public HugeTypes type() {
        return HugeTypes.PROPERTY;
    }

    @Override
    public String name() {
        return this.key;
    }

    @Override
    public String key() {
        return this.key;
    }

    @Override
    public V value() throws NoSuchElementException {
        return this.value;
    }

    @Override
    public boolean isPresent() {
        return null != this.value;
    }

    @Override
    public Element element() {
        return this.owner;
    }

    @Override
    public void remove() {
        throw Property.Exceptions.propertyRemovalNotSupported();
    }

    @Override
    public String toString() {
        return String.format("{type=%s, value=%s}", this.key, this.value);
    }
}
