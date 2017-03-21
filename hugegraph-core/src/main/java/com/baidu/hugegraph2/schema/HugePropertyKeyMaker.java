package com.baidu.hugegraph2.schema;

import com.baidu.hugegraph2.Cardinality;
import com.baidu.hugegraph2.DataType;
import com.baidu.hugegraph2.backend.store.SchemaStore;
import com.baidu.hugegraph2.schema.base.maker.PropertyKeyMaker;
import com.baidu.hugegraph2.schema.base.PropertyKey;
import com.baidu.hugegraph2.schema.base.SchemaType;

/**
 * Created by jishilei on 17/3/17.
 */
public class HugePropertyKeyMaker implements PropertyKeyMaker {

    private SchemaStore schemaStore;
    private HugePropertyKey propertyKey;
    private String name;

    public HugePropertyKeyMaker(SchemaStore schemaStore, String name) {
        this.name = name;
        this.schemaStore = schemaStore;
        propertyKey = new HugePropertyKey(name);
    }

    public PropertyKey getPropertyKey() {
        return propertyKey;
    }

    @Override
    public PropertyKeyMaker asText() {
        this.propertyKey.setDataType(DataType.TEXT);
        return this;
    }

    @Override
    public PropertyKeyMaker asInt() {
        this.propertyKey.setDataType(DataType.INT);
        return this;
    }

    @Override
    public PropertyKeyMaker asTimeStamp() {
        this.propertyKey.setDataType(DataType.TIMESTAMP);
        return this;
    }

    @Override
    public PropertyKeyMaker single() {
        this.propertyKey.setCardinality(Cardinality.SINGLE);
        return this;
    }

    @Override
    public PropertyKeyMaker multiple() {
        this.propertyKey.setCardinality(Cardinality.MULTIPLE);
        return this;
    }

    @Override
    public PropertyKeyMaker properties(String propertyName) {
        this.propertyKey.addProperties(propertyName);
        return this;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public PropertyKey create() {
        schemaStore.addPropertyKey(propertyKey);
        return propertyKey;
    }

    @Override
    public SchemaType add() {
        schemaStore.removePropertyKey(name);
        schemaStore.addPropertyKey(propertyKey);
        return propertyKey;
    }

    @Override
    public void remove() {
        schemaStore.removePropertyKey(name);
    }
}
