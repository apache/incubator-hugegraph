package com.baidu.hugegraph2.type.schema;

import java.util.Set;

import com.baidu.hugegraph2.type.HugeTypes;
import com.baidu.hugegraph2.type.define.Cardinality;
import com.baidu.hugegraph2.type.define.Multiplicity;

/**
 * Created by jishilei on 17/3/18.
 */
public interface EdgeLabel extends SchemaType {

    public Set<String> properties();

    public Cardinality cardinality();

    public Multiplicity multiplicity();

    public boolean isDirected();

    public void partitionKeys(String... keys);

    public boolean hasPartitionKeys();

    @Override
    public default HugeTypes type() {
        return HugeTypes.EDGE_LABEL;
    }
}
