package com.baidu.hugegraph2.type.schema;

import java.util.Set;

import com.baidu.hugegraph2.Cardinality;
import com.baidu.hugegraph2.DataType;
import com.baidu.hugegraph2.type.HugeTypes;

/**
 * Created by jishilei on 17/3/17.
 */
public interface PropertyKey extends SchemaType {

    public DataType dataType();

    public Cardinality cardinality();

    public Set<String> properties();

    @Override
    public default HugeTypes type() {
        return HugeTypes.PROPERTY_KEY;
    }
}
