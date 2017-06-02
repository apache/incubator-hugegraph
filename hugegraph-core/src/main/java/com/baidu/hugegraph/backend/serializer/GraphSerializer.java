package com.baidu.hugegraph.backend.serializer;

import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.structure.HugeIndex;
import com.baidu.hugegraph.structure.HugeVertex;

public interface GraphSerializer {

    public BackendEntry writeVertex(HugeVertex vertex);
    public HugeVertex readVertex(BackendEntry entry);

    // public BackendEntry writeEdge(HugeEdge edge);
    // public HugeEdge readEdge(BackendEntry entry);

    public BackendEntry writeIndex(HugeIndex index);
    public HugeIndex readIndex(BackendEntry entry);
}
