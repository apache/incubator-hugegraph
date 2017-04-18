package com.baidu.hugegraph.backend.serializer;

import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.type.schema.EdgeLabel;
import com.baidu.hugegraph.type.schema.PropertyKey;
import com.baidu.hugegraph.type.schema.VertexLabel;

public interface SchemaSerializer {

    public BackendEntry writeVertexLabel(VertexLabel vertexLabel);
    public BackendEntry writeEdgeLabel(EdgeLabel edgeLabel);
    public BackendEntry writePropertyKey(PropertyKey propertyKey);

    public VertexLabel readVertexLabel(BackendEntry entry);
    public EdgeLabel readEdgeLabel(BackendEntry entry);
    public PropertyKey readPropertyKey(BackendEntry entry);
}
