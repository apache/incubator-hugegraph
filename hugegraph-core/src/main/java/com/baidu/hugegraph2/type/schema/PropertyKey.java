package com.baidu.hugegraph2.type.schema;

import java.util.Set;

import com.baidu.hugegraph2.backend.tx.SchemaTransaction;
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

    public abstract DataType dataType();

    public abstract Cardinality cardinality();

    public abstract PropertyKey asText();

    public abstract PropertyKey asInt();

    public abstract PropertyKey asTimestamp();

    public abstract PropertyKey asUuid();

    public abstract PropertyKey single();

    public abstract PropertyKey multiple();
}
