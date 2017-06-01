package com.baidu.hugegraph.type.schema;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;

import com.baidu.hugegraph.schema.HugePropertyKey;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.type.HugeType;

/**
 * Created by jishilei on 17/3/18.
 */
public abstract class VertexLabel extends SchemaElement {

    public VertexLabel(String name) {
        super(name);
    }

    @Override
    public HugeType type() {
        return HugeType.VERTEX_LABEL;
    }

    @Override
    public VertexLabel properties(String... propertyNames) {
        this.properties.addAll(Arrays.asList(propertyNames));
        return this;
    }

    public abstract Set<String> primaryKeys();

    public abstract VertexLabel primaryKeys(String... keys);

    @Override
    public VertexLabel ifNotExist() {
        this.checkExits = false;
        return this;
    }

    @Override
    public abstract VertexLabel create();
}
