package com.baidu.hugegraph.backend.serializer;

import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.type.schema.EdgeLabel;
import com.baidu.hugegraph.type.schema.IndexLabel;
import com.baidu.hugegraph.type.schema.PropertyKey;
import com.baidu.hugegraph.type.schema.VertexLabel;

public interface SchemaSerializer {

    public BackendEntry writeVertexLabel(VertexLabel vertexLabel);
    public VertexLabel readVertexLabel(BackendEntry entry);

    public BackendEntry writeEdgeLabel(EdgeLabel edgeLabel);
    public EdgeLabel readEdgeLabel(BackendEntry entry);

    public BackendEntry writePropertyKey(PropertyKey propertyKey);
    public PropertyKey readPropertyKey(BackendEntry entry);

    public BackendEntry writeIndexLabel(IndexLabel indexLabel);
    public IndexLabel readIndexLabel(BackendEntry entry);
}
