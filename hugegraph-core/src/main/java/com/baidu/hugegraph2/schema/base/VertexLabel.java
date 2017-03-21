package com.baidu.hugegraph2.schema.base;

import java.util.Set;

import com.baidu.hugegraph2.IndexType;

/**
 * Created by jishilei on 17/3/18.
 */
public interface VertexLabel extends SchemaType {

    public Set<String> properties();

    public IndexType indexType();

    public void index(String indexName);
}
