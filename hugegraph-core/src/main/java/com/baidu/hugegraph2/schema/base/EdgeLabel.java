package com.baidu.hugegraph2.schema.base;

import java.util.Set;

import com.baidu.hugegraph2.Cardinality;
import com.baidu.hugegraph2.Multiplicity;

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
}
