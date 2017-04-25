package com.baidu.hugegraph.backend.serializer;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.type.HugeTypes;

public abstract class AbstractSerializer implements VertexSerializer, SchemaSerializer, IndexSerializer {

    protected HugeGraph graph = null;

    public AbstractSerializer(HugeGraph graph) {
        this.graph=graph;
    }

    protected BackendEntry convertEntry(BackendEntry entry) {
        return entry;
    }

    public abstract BackendEntry newBackendEntry(Id id);

    public abstract BackendEntry writeId(HugeTypes type, Id id);

    public abstract Query writeQuery(Query query);
}
