package com.baidu.hugegraph2.structure;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import com.baidu.hugegraph2.backend.id.Id;
import com.baidu.hugegraph2.backend.id.IdGenerator;
import com.baidu.hugegraph2.type.schema.VertexLabel;
import com.google.common.base.Preconditions;

/**
 * Created by jishilei on 17/3/16.
 */
public abstract class HugeElement implements Element, GraphType {

    protected final Graph graph;
    protected Id id;
    protected VertexLabel label;
    protected Map<String, HugeProperty<? extends Object>> properties;

    public HugeElement(final Graph graph, final Id id, final VertexLabel label) {
        this.graph = graph;
        this.id = id;
        this.label = label;
        this.properties = new HashMap<>();
    }

    @Override
    public Id id() {
        return this.id;
    }

    @Override
    public String label() {
        return this.label.name();
    }

    public VertexLabel vertexLabel() {
        return this.label;
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

    public HugeProperty<? extends Object> setProperty(HugeProperty<?> prop) {
        return this.properties.put(prop.key(), prop);
    }

    public static Id getIdValue(Object... keyValues) {
        Optional<Object> id = ElementHelper.getIdValue(keyValues);
        if (id.isPresent()) {
            Object idValue = id.get();
            // number id
            if (idValue instanceof Number) {
                return IdGenerator.generate(((Number) idValue).longValue());
            }
            // string id
            else if (idValue instanceof String) {
                return IdGenerator.generate((String) idValue);
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
