package com.baidu.hugegraph.backend.serializer;

import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.structure.HugeVertex;

public interface VertexSerializer {

    public BackendEntry writeVertex(HugeVertex vertex);

    public HugeVertex readVertex(BackendEntry entry);
}
