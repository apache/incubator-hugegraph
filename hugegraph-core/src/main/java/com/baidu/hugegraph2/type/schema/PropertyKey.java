package com.baidu.hugegraph2.type.schema;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
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
        if (this.properties == null) {
            this.properties = new HashMap<>();
        }
        for (String propertyName : propertyNames) {
            this.properties.put(propertyName,
                    new HugePropertyKey(propertyName, this.transaction));
        }
        return this;
    }

    public Class<?> clazz() {
        Class<?> dataType = this.dataType().clazz();
        Class<?> cls = null;

        switch (this.cardinality()) {
            case SINGLE:
                cls = dataType;
                break;
            case SET: // a set of values: Set<DataType>
                cls = LinkedHashSet.class;
                break;
            case LIST: // a list of values: List<DataType>
                cls = LinkedList.class;
                break;
            default:
                assert false;
                break;
        }
        return cls;
    }

    // check type of the value valid
    public <V> boolean checkDataType(V value) {
        return this.dataType().clazz().isInstance(value);
    }

    // check type of all the values(may be some of list properties) valid
    public <V> boolean checkDataType(Collection<V> values) {
        boolean valid = true;
        for (Object o : values) {
            if (!this.checkDataType(o)) {
                valid = false;
                break;
            }
        }
        return valid;
    }

    // check property value valid
    public <V> boolean checkValue(V value) {
        boolean valid = false;

        switch (this.cardinality()) {
            case SINGLE:
                valid = this.checkDataType(value);
                break;
            case SET:
                valid = value instanceof Set;
                valid = valid && this.checkDataType((Set<?>) value);
                break;
            case LIST:
                valid = value instanceof List;
                valid = valid && this.checkDataType((List<?>) value);
                break;
            default:
                assert false;
                break;
        }
        return valid;
    }

    public abstract DataType dataType();

    public abstract Cardinality cardinality();

    public abstract PropertyKey asText();

    public abstract PropertyKey asInt();

    public abstract PropertyKey asTimestamp();

    public abstract PropertyKey asUuid();

    public abstract PropertyKey valueSingle();

    public abstract PropertyKey valueList();

    public abstract PropertyKey valueSet();
}
