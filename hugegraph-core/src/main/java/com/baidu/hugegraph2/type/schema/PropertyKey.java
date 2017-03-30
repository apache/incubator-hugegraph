package com.baidu.hugegraph2.type.schema;

import java.util.HashMap;
import java.util.Set;

import com.baidu.hugegraph2.backend.tx.SchemaTransaction;
import com.baidu.hugegraph2.schema.HugePropertyKey;
import com.baidu.hugegraph2.schema.SchemaElement;
import com.baidu.hugegraph2.type.HugeTypes;
import com.baidu.hugegraph2.type.define.Cardinality;
import com.baidu.hugegraph2.type.define.DataType;

/**
 * Created by jishilei on 17/3/17.
 */
public abstract class PropertyKey extends SchemaElement {

    public PropertyKey(String name, SchemaTransaction transaction) {
        super(name, transaction);
    }

    @Override
    public HugeTypes type() {
        return HugeTypes.PROPERTY_KEY;
    }

    @Override
    public PropertyKey properties(String... propertyNames) {
        if (properties == null) {
            properties = new HashMap<>();
        }
        for (String propertyName : propertyNames) {
            properties.put(propertyName, new HugePropertyKey(propertyName, transaction));
        }
        return this;
    }

    public abstract DataType dataType();

    public abstract Cardinality cardinality();

    public abstract PropertyKey asText();

    public abstract PropertyKey asInt();

    public abstract PropertyKey asTimestamp();

    public abstract PropertyKey asUuid();

    public abstract PropertyKey valueSingle();

    public abstract PropertyKey valueRepeatable();

    public abstract PropertyKey valueUnrepeatable();
}
