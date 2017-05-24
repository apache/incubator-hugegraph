package com.baidu.hugegraph.core;

import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

public class FakeObjects {

    public static class FakeVertex {

        private String label;
        private Map<String, Object> values;

        public FakeVertex(Object... keyValues) {
            this.label = ElementHelper.getLabelValue(keyValues).get();
            this.values = ElementHelper.asMap(keyValues);

            this.values.remove("label");
        }

        public boolean equalsVertex(Vertex v) {
            if (!v.label().equals(this.label)) {
                return false;
            }
            for (Map.Entry<String, Object> i : this.values.entrySet()) {
                if (!v.property(i.getKey()).value().equals(i.getValue())) {
                    return false;
                }
            }
            return true;
        }
    }
}
