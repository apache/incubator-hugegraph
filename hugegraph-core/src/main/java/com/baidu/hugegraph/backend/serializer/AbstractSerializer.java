package com.baidu.hugegraph.backend.serializer;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.type.HugeType;

public abstract class AbstractSerializer
        implements GraphSerializer, SchemaSerializer {

    protected HugeGraph graph = null;

    public AbstractSerializer(HugeGraph graph) {
        this.graph = graph;
    }

    protected BackendEntry convertEntry(BackendEntry entry) {
        return entry;
    }

    public abstract BackendEntry newBackendEntry(Id id);

    public abstract BackendEntry writeId(HugeType type, Id id);

    public abstract Query writeQuery(Query query);
}
