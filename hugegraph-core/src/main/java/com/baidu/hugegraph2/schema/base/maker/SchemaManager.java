package com.baidu.hugegraph2.schema.base.maker;

import com.baidu.hugegraph2.backend.BackendException;
import com.baidu.hugegraph2.schema.base.VertexLabel;

/**
 * Created by jishilei on 17/3/17.
 */
public interface SchemaManager {

    public PropertyKeyMaker propertyKey(String name);

    public VertexLabelMaker vertexLabel(String name);

    public EdgeLabelMaker edgeLabel(String name);

    public void desc();

    public VertexLabel getOrCreateVertexLabel(String label);

    public void commit() throws BackendException;

    public void rollback() throws BackendException;
}
