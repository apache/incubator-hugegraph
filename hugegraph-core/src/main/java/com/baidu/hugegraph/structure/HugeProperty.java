package com.baidu.hugegraph.structure;

import java.util.NoSuchElementException;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.schema.PropertyKey;
import com.google.common.base.Preconditions;

/**
 * Created by jishilei on 17/3/16.
 */
public class HugeProperty<V> implements Property<V>, GraphType {

    protected final HugeElement owner;
    protected final PropertyKey key;
    protected final V value;

    public HugeProperty(final HugeElement owner, PropertyKey key, V value) {
        Preconditions.checkArgument(owner != null, "Property owner can't be null");
        Preconditions.checkArgument(key != null, "Property key can't be null");

        this.owner = owner;
        this.key = key;
        this.value = value;

        Preconditions.checkArgument(key.checkValue(value), String.format(
                "Invalid property value '%s' for key '%s'", value, key.name()));
    }

    public PropertyKey propertyKey() {
        return this.key;
    }

    @Override
    public HugeType type() {
        return HugeType.PROPERTY;
    }

    @Override
    public String name() {
        return this.key.name();
    }

    @Override
    public String key() {
        return this.key.name();
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
    public boolean equals(Object obj) {
        if (!(obj instanceof HugeProperty)) {
            return false;
        }

        HugeProperty<?> other = (HugeProperty<?>) obj;
        return this.owner.equals(other.owner) && this.key.equals(other.key);
    }

    @Override
    public int hashCode() {
        return this.owner.hashCode() ^ this.key.hashCode();
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }
}
