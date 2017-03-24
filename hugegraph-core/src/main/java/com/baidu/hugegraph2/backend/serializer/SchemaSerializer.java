package com.baidu.hugegraph2.backend.serializer;

import com.baidu.hugegraph2.backend.store.BackendEntry;
import com.baidu.hugegraph2.type.schema.EdgeLabel;
import com.baidu.hugegraph2.type.schema.PropertyKey;
import com.baidu.hugegraph2.type.schema.VertexLabel;

public interface SchemaSerializer {

    public BackendEntry writeVertexLabel(VertexLabel vertexLabel);
    public BackendEntry writeEdgeLabel(EdgeLabel edgeLabel);
    public BackendEntry writePropertyKey(PropertyKey propertyKey);

    public VertexLabel readVertexLabel(BackendEntry entry);
    public EdgeLabel readEdgeLabel(BackendEntry entry);
    public PropertyKey readPropertyKey(BackendEntry entry);
}
