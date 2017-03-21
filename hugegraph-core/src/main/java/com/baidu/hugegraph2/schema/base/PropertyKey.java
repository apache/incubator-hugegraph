package com.baidu.hugegraph2.schema.base;

import java.util.Set;

import com.baidu.hugegraph2.Cardinality;
import com.baidu.hugegraph2.DataType;

/**
 * Created by jishilei on 17/3/17.
 */
public interface PropertyKey extends SchemaType {

    public DataType dataType();

    public Cardinality cardinality();

    public Set<String> properties();
}
