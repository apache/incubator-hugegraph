package com.baidu.hugegraph.schema;

import com.baidu.hugegraph.type.schema.EdgeLabel;
import com.baidu.hugegraph.type.schema.PropertyKey;
import com.baidu.hugegraph.type.schema.VertexLabel;

/**
 * Created by liningrui on 2017/3/25.
 */
public interface SchemaManager {

    public PropertyKey propertyKey(String name);

    public VertexLabel vertexLabel(String name);

    public EdgeLabel edgeLabel(String name);

    public void desc();

}
