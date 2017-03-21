package com.baidu.hugegraph2.schema.base;

import com.baidu.hugegraph2.Cardinality;
import com.baidu.hugegraph2.Multiplicity;

/**
 * Created by jishilei on 17/3/18.
 */
public interface EdgeLabel extends SchemaType {

    public Cardinality cardinality();

    public Multiplicity multiplicity();

    public boolean isDirected();

    public void addPartitionKeys(String... keys);

    public boolean hasPartitionKeys();
}
