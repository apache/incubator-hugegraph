package com.baidu.hugegraph2.schema;

import com.baidu.hugegraph2.type.schema.EdgeLabel;
import com.baidu.hugegraph2.type.schema.PropertyKey;
import com.baidu.hugegraph2.type.schema.VertexLabel;

/**
 * Created by liningrui on 2017/3/25.
 */
public interface SchemaManager {

    public PropertyKey propertyKey(String name);

    public VertexLabel vertexLabel(String name);

    public EdgeLabel edgeLabel(String name);

    public void desc();

    public VertexLabel getOrCreateVertexLabel(String label);
    public VertexLabel getVertexLabel(String label);

    public boolean commit();
}
