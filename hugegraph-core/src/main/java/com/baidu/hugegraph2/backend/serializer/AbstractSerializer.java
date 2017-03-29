package com.baidu.hugegraph2.backend.serializer;

import com.baidu.hugegraph2.HugeGraph;
import com.baidu.hugegraph2.backend.id.Id;
import com.baidu.hugegraph2.backend.store.BackendEntry;

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
