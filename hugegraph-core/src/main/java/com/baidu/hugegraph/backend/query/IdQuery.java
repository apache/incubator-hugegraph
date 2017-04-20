package com.baidu.hugegraph.backend.query;

import java.util.LinkedHashSet;
import java.util.Set;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.type.HugeTypes;

public class IdQuery extends Query {

    // ids will be concated with `or`
    private Set<Id> ids;

    public IdQuery(HugeTypes resultType) {
        super(resultType);
        this.ids = new LinkedHashSet<>();
    }

    @Override
    public Set<Id> ids() {
        return this.ids;
    }

    public IdQuery query(Id id) {
        this.ids.add(id);
        return this;
    }
}
