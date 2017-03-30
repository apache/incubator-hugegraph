package com.baidu.hugegraph2.structure;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import com.baidu.hugegraph2.HugeGraph;
import com.baidu.hugegraph2.backend.id.Id;
import com.baidu.hugegraph2.backend.id.IdGeneratorFactory;
import com.baidu.hugegraph2.type.schema.VertexLabel;
import com.google.common.base.Preconditions;

/**
 * Created by jishilei on 17/3/16.
 */
public abstract class HugeElement implements Element, GraphType {

    protected final HugeGraph graph;
    protected Id id;
    protected Map<String, HugeProperty<? extends Object>> properties;

    public HugeElement(final HugeGraph graph, final Id id) {
        this.graph = graph;
        this.id = id;
        this.properties = new HashMap<>();
    }

    @Override
    public Id id() {
        return this.id;
    }

    @Override
    public Graph graph() {
        return this.graph;
    }

    public Map<String, HugeProperty<? extends Object>> getProperties() {
        return this.properties;
    }

    public <V> HugeProperty<? extends Object> getProperty(String key) {
        return this.properties.get(key);
    }

    public boolean existsProperty(String key) {
        return this.properties.containsKey(key);
    }

    public <V> HugeProperty<? extends Object> setProperty(HugeProperty<V> prop) {
        return this.properties.put(prop.key(), prop);
    }

    public static Id getIdValue(Object... keyValues) {
        Optional<Object> id = ElementHelper.getIdValue(keyValues);
        if (id.isPresent()) {
            Object idValue = id.get();
            // number id
            if (idValue instanceof Number) {
                return IdGeneratorFactory.generator().generate(((Number) idValue).longValue());
            }
            // string id
            else if (idValue instanceof String) {
                return IdGeneratorFactory.generator().generate((String) idValue);
            }
            // error
            String msg = "Not supported id type(must be a number or string): ";
            msg += idValue.getClass().getSimpleName();
            throw new UnsupportedOperationException(msg);
        }
        return null;
    }

    public static Object getLabelValue(Object... keyValues) {
        Object labelValue = null;
        for (int i = 0; i < keyValues.length; i = i + 2) {
            if (keyValues[i].equals(T.label)) {
                labelValue = keyValues[i + 1];
                Preconditions.checkArgument(
                        labelValue instanceof VertexLabel || labelValue instanceof String,
                        "Expected a string or VertexLabel as the vertex label argument,"
                                + "but got: %s", labelValue);
                if (labelValue instanceof String) {
                    ElementHelper.validateLabel((String) labelValue);
                }
            }
        }
        return labelValue;
    }
}
