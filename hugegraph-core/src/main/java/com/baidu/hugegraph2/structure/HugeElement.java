package com.baidu.hugegraph2.structure;

import java.util.HashMap;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;


/**
 * Created by jishilei on 17/3/16.
 */
public abstract class HugeElement implements Element {

    protected final Object id;
    protected final String label;
    private final Graph graph;
    protected Map<String, Object> properties;

    public HugeElement(final Graph graph, final Object id, final String label) {
        this.graph = graph;
        this.id = id;
        this.label = label;
    }

    @Override
    public Object id() {
        return this.id;
    }

    @Override
    public String label() {
        return this.label;
    }

    @Override
    public Graph graph() {
        return this.graph;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        if (this.properties == null) {
            this.properties = new HashMap<>();
        }
        for (int i = 0; i < keyValues.length; i = i + 2) {
            if (!keyValues[i].equals(T.id) && !keyValues[i].equals(T.label)) {
                this.properties.put((String) keyValues[i], keyValues[i + 1]);
            }
        }

    }

    public void setPropertyMap(Map<String, Object> map) {
        this.properties = map;
    }

    public <V> V getProperty(String key) {
        if (properties != null) {
            V val = (V) properties.get(key);
            if (val != null) {
                return val;
            }
        }
        return null;
    }
}
