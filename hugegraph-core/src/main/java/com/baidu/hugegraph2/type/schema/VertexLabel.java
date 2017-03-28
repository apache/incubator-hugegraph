package com.baidu.hugegraph2.type.schema;

import java.util.LinkedHashSet;
import java.util.Set;

import com.baidu.hugegraph2.backend.tx.SchemaTransaction;
import com.baidu.hugegraph2.schema.SchemaElement;
import com.baidu.hugegraph2.type.HugeTypes;
import com.baidu.hugegraph2.type.define.IndexType;

/**
 * Created by jishilei on 17/3/18.
 */
public abstract class VertexLabel extends SchemaElement {

    public VertexLabel(String name, SchemaTransaction transaction) {
        super(name, transaction);
    }

    public abstract Set<String> primaryKeys();

    @Override
    public HugeTypes type() {
        return HugeTypes.VERTEX_LABEL;
    }

    public abstract IndexType indexType();

    public abstract void index(String indexName);

    public abstract VertexLabel partitionKey(String... keys);

    public abstract VertexLabel clusteringKey(String... keys);
}
