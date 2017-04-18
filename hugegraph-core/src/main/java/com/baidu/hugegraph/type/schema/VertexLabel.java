package com.baidu.hugegraph.type.schema;

import java.util.HashMap;
import java.util.Set;

import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.schema.HugePropertyKey;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.type.HugeTypes;

/**
 * Created by jishilei on 17/3/18.
 */
public abstract class VertexLabel extends SchemaElement {

    public VertexLabel(String name, SchemaTransaction transaction) {
        super(name, transaction);
    }

    @Override
    public HugeTypes type() {
        return HugeTypes.VERTEX_LABEL;
    }

    @Override
    public VertexLabel properties(String... propertyNames) {
        if (this.properties == null) {
            this.properties = new HashMap<>();
        }
        for (String propertyName : propertyNames) {
            this.properties.put(propertyName, new HugePropertyKey(propertyName, this.transaction));
        }
        return this;
    }

    public abstract Set<String> primaryKeys();

    public abstract VertexLabel primaryKeys(String... keys);

}
