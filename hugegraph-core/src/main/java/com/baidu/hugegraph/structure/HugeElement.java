package com.baidu.hugegraph.structure;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGeneratorFactory;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.type.schema.PropertyKey;
import com.baidu.hugegraph.type.schema.VertexLabel;
import com.google.common.base.Preconditions;

/**
 * Created by jishilei on 17/3/16.
 */
public abstract class HugeElement implements Element, GraphType {

    protected final HugeGraph graph;
    protected boolean removed;
    protected Id id;
    protected Map<String, HugeProperty<? extends Object>> properties;

    public HugeElement(final HugeGraph graph, final Id id) {
        this.graph = graph;
        this.id = id;
        this.properties = new HashMap<>();
        this.removed = false;
    }

    @Override
    public Id id() {
        return this.id;
    }

    @Override
    public Graph graph() {
        return this.graph;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof HugeElement)) {
            return false;
        }

        HugeElement other = (HugeElement) obj;
        if (this.id() == null) {
            return false;
        }

        return this.id().equals(other.id());
    }

    public boolean removed() {
        return this.removed;
    }

    public abstract GraphTransaction tx();

    public Map<String, HugeProperty<? extends Object>> getProperties() {
        return this.properties;
    }

    public <V> HugeProperty<V> getProperty(String key) {
        return (HugeProperty<V>) this.properties.get(key);
    }

    public boolean existsProperty(String key) {
        return this.properties.containsKey(key);
    }

    public boolean existsProperties() {
        return this.properties.size() > 0;
    }

    public <V> void setProperty(HugeProperty<V> prop) {
        this.properties.put(prop.key(), prop);
    }

    public <V> HugeProperty<V> addProperty(String key, V value) {
        HugeProperty<V> prop = null;
        PropertyKey pkey = this.graph.schema().propertyKey(key);
        switch (pkey.cardinality()) {
            case SINGLE:
                prop = this.newProperty(pkey, value);
                this.setProperty(prop);
                break;
            case SET:
                Preconditions.checkArgument(pkey.checkDataType(value), String.format(
                        "Invalid property value '%s' for key '%s'", value, key));

                HugeProperty<Set<V>> propSet;
                if (this.existsProperty(key)) {
                    propSet = this.<Set<V>>getProperty(key);
                } else {
                    propSet = this.newProperty(pkey, new LinkedHashSet<V>());
                    this.setProperty(propSet);
                }

                propSet.value().add(value);

                // any better ways?
                prop = (HugeProperty) propSet;
                break;
            case LIST:
                Preconditions.checkArgument(pkey.checkDataType(value), String.format(
                        "Invalid property value '%s' for key '%s'", value, key));

                HugeProperty<List<V>> propList;
                if (!this.existsProperty(key)) {
                    propList = this.newProperty(pkey, new LinkedList<V>());
                    this.setProperty(propList);
                } else {
                    propList = this.<List<V>>getProperty(key);
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

    protected <V> HugeProperty<V> newProperty(PropertyKey pkey, V value) {
        return new HugeProperty<>(this, pkey, value);
    }

    public void resetProperties() {
        this.properties = new HashMap<>();
    }

    public static Id getIdValue(Object... keyValues) {
        Optional<Object> id = ElementHelper.getIdValue(keyValues);
        if (!id.isPresent()) {
            return null;
        }

        Object idValue = id.get();
        // number id
        if (idValue instanceof Number) {
            return IdGeneratorFactory.generator().generate(
                    ((Number) idValue).longValue());
        }
        // string id
        else if (idValue instanceof String) {
            return IdGeneratorFactory.generator().generate((String) idValue);
        }
        // id itself
        else if (idValue instanceof Id) {
            return (Id) idValue;
        }
        // error type
        String msg = "Unsupported id type(must be a number or string): ";
        msg += idValue.getClass().getSimpleName();
        throw new UnsupportedOperationException(msg);
    }

    public static Object getLabelValue(Object... keyValues) {
        Object labelValue = null;
        for (int i = 0; i < keyValues.length; i = i + 2) {
            if (keyValues[i].equals(T.label)) {
                labelValue = keyValues[i + 1];
                Preconditions.checkArgument(labelValue instanceof VertexLabel
                        || labelValue instanceof String,
                        "Expected a string or VertexLabel as the vertex label"
                        + " argument, but got: %s", labelValue);
                if (labelValue instanceof String) {
                    ElementHelper.validateLabel((String) labelValue);
                }
            }
        }
        return labelValue;
    }
}
