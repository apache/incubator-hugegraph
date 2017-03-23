package com.baidu.hugegraph2.schema;

import com.baidu.hugegraph2.backend.tx.SchemaTransaction;
import com.baidu.hugegraph2.schema.maker.PropertyKeyMaker;
import com.baidu.hugegraph2.type.define.Cardinality;
import com.baidu.hugegraph2.type.define.DataType;
import com.baidu.hugegraph2.type.schema.PropertyKey;
import com.baidu.hugegraph2.type.schema.SchemaType;

/**
 * Created by jishilei on 17/3/17.
 */
public class HugePropertyKeyMaker implements PropertyKeyMaker {

    private String name;
    private SchemaTransaction transaction;
    private HugePropertyKey propertyKey;

    public HugePropertyKeyMaker(SchemaTransaction transaction, String name) {
        this.name = name;
        this.transaction = transaction;
        propertyKey = new HugePropertyKey(name);
    }

    public PropertyKey propertyKey() {
        return propertyKey;
    }

    @Override
    public PropertyKeyMaker asText() {
        this.propertyKey.dataType(DataType.TEXT);
        return this;
    }

    @Override
    public PropertyKeyMaker asInt() {
        this.propertyKey.dataType(DataType.INT);
        return this;
    }

    @Override
    public PropertyKeyMaker asTimeStamp() {
        this.propertyKey.dataType(DataType.TIMESTAMP);
        return this;
    }

    @Override
    public PropertyKeyMaker asUUID() {
        this.propertyKey.dataType(DataType.UUID);
        return this;
    }

    @Override
    public PropertyKeyMaker single() {
        this.propertyKey.cardinality(Cardinality.SINGLE);
        return this;
    }

    @Override
    public PropertyKeyMaker multiple() {
        this.propertyKey.cardinality(Cardinality.MULTIPLE);
        return this;
    }

    @Override
    public PropertyKeyMaker properties(String... propertyNames) {
        this.propertyKey.properties(propertyNames);
        return this;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public PropertyKey create() {
        transaction.addPropertyKey(propertyKey);
        return propertyKey;
    }

    @Override
    public SchemaType add() {
        transaction.removePropertyKey(name);
        transaction.addPropertyKey(propertyKey);
        return propertyKey;
    }

    @Override
    public void remove() {
        transaction.removePropertyKey(name);
    }
}
