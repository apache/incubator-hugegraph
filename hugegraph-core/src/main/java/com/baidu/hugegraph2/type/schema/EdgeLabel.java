package com.baidu.hugegraph2.type.schema;

import java.util.LinkedHashSet;
import java.util.Set;

import com.baidu.hugegraph2.backend.tx.SchemaTransaction;
import com.baidu.hugegraph2.schema.SchemaElement;
import com.baidu.hugegraph2.type.HugeTypes;
import com.baidu.hugegraph2.type.define.Cardinality;
import com.baidu.hugegraph2.type.define.Multiplicity;

/**
 * Created by jishilei on 17/3/18.
 */
public abstract class EdgeLabel extends SchemaElement {

    public EdgeLabel(String name, SchemaTransaction transaction) {
        super(name, transaction);
    }

    public abstract Set<String> sortKeys();

    @Override
    public HugeTypes type() {
        return HugeTypes.EDGE_LABEL;
    }

    public abstract Cardinality cardinality();

    public abstract Multiplicity multiplicity();

    public abstract boolean isDirected();

    public abstract void partitionKeys(String... keys);

    public abstract boolean hasPartitionKeys();

    public abstract EdgeLabel linkOne2One();

    public abstract EdgeLabel linkOne2Many();

    public abstract EdgeLabel linkMany2Many();

    public abstract EdgeLabel linkMany2One();

    public abstract EdgeLabel single();

    public abstract EdgeLabel multiple();

    public abstract EdgeLabel link(String srcName, String tgtName);
}
