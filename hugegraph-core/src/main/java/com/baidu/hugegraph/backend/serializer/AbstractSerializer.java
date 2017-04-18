package com.baidu.hugegraph.backend.serializer;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.store.BackendEntry;

public abstract class AbstractSerializer implements VertexSerializer, SchemaSerializer {

    protected HugeGraph graph = null;

    public AbstractSerializer(HugeGraph graph) {
        this.graph=graph;
    }

    public abstract BackendEntry newBackendEntry(Id id);

    protected BackendEntry convertEntry(BackendEntry entry) {
        return entry;
    }
}
