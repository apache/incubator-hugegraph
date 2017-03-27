package com.baidu.hugegraph2.type.schema;

import java.util.Set;

import com.baidu.hugegraph2.type.HugeTypes;
import com.baidu.hugegraph2.type.define.IndexType;

/**
 * Created by jishilei on 17/3/18.
 */
public interface VertexLabel extends SchemaType {

    public Set<String> properties();

    public IndexType indexType();

    public void index(String indexName);

    @Override
    public default HugeTypes type() {
        return HugeTypes.VERTEX_LABEL;
    }

    public VertexLabel partitionKey(String... keys);

    public VertexLabel clusteringKey(String... keys);
}
