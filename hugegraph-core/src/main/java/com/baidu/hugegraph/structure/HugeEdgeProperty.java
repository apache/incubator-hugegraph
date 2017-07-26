package com.baidu.hugegraph.structure;

import com.baidu.hugegraph.type.schema.PropertyKey;

public class HugeEdgeProperty<V> extends HugeProperty<V> {

    public HugeEdgeProperty(HugeElement owner, PropertyKey key, V value) {
        super(owner, key, value);
    }

    @Override
    public HugeEdge element() {
        assert super.element() instanceof HugeEdge;
        return (HugeEdge) super.element();
    }

    @Override
    public void remove() {
        this.owner.removeProperty(this.key());
        this.owner.tx().removeEdgeProperty(this);
    }
}
