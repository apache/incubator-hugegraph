package com.baidu.hugegraph.backend.serializer;

import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.structure.HugeIndex;
import com.baidu.hugegraph.type.define.IndexType;

/**
 * Created by liningrui on 2017/4/25.
 */
public interface IndexSerializer {

    public BackendEntry writeIndex(HugeIndex index);

    public HugeIndex readIndex(BackendEntry entry, IndexType indexType);

}
