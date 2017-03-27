package com.baidu.hugegraph2.type.schema;

import java.util.Set;

import com.baidu.hugegraph2.type.HugeTypes;
import com.baidu.hugegraph2.type.define.Cardinality;
import com.baidu.hugegraph2.type.define.DataType;

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

    public PropertyKey asText();

    public PropertyKey asInt();

    public PropertyKey asTimeStamp();

    public PropertyKey asUUID();

    public PropertyKey single();

    public PropertyKey multiple();
}
