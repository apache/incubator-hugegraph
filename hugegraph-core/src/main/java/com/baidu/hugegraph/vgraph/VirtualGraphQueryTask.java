package com.baidu.hugegraph.vgraph;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.type.HugeType;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class VirtualGraphQueryTask<T extends VirtualElement> {

    public VirtualGraphQueryTask(HugeType hugeType, List<Id> ids) {
        assert hugeType == HugeType.VERTEX || hugeType == HugeType.EDGE;
        assert ids != null;

        this.hugeType = hugeType;
        this.ids = ids;
        this.future = new CompletableFuture<>();
    }

    public HugeType getHugeType() {
        return hugeType;
    }

    public List<Id> getIds() {
        return ids;
    }

    public CompletableFuture<Iterator<T>> getFuture() {
        return future;
    }

    private final HugeType hugeType;
    private final List<Id> ids;
    private final CompletableFuture<Iterator<T>> future;
}
