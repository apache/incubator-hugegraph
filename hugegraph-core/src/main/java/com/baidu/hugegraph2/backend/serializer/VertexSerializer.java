package com.baidu.hugegraph2.backend.serializer;

import com.baidu.hugegraph2.backend.store.BackendEntry;
import com.baidu.hugegraph2.structure.HugeVertex;

public interface VertexSerializer {

    public BackendEntry writeVertex(HugeVertex vertex);

    public HugeVertex readVertex(BackendEntry entry);
}
