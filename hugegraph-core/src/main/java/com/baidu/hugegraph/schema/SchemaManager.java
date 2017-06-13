package com.baidu.hugegraph.schema;

import java.util.List;

import com.baidu.hugegraph.type.schema.EdgeLabel;
import com.baidu.hugegraph.type.schema.IndexLabel;
import com.baidu.hugegraph.type.schema.PropertyKey;
import com.baidu.hugegraph.type.schema.VertexLabel;

/**
 * Created by liningrui on 2017/3/25.
 */
public interface SchemaManager {

    public PropertyKey makePropertyKey(String name);

    public VertexLabel makeVertexLabel(String name);

    public EdgeLabel makeEdgeLabel(String name);

    public PropertyKey propertyKey(String name);

    public VertexLabel vertexLabel(String name);

    public EdgeLabel edgeLabel(String name);

    public IndexLabel makeIndexLabel(String name);

    public List<SchemaElement> desc();

    public SchemaElement create(SchemaElement element);

    public SchemaElement append(SchemaElement element);
}
