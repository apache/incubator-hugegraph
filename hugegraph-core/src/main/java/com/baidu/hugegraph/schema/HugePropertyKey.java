package com.baidu.hugegraph.schema;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.define.DataType;
import com.baidu.hugegraph.type.schema.PropertyKey;
import com.baidu.hugegraph.util.StringUtil;

/**
 * Created by jishilei on 17/3/17.
 */
public class HugePropertyKey extends PropertyKey {

    private static final Logger logger = LoggerFactory.getLogger(HugePropertyKey.class);

    private DataType dataType;
    private Cardinality cardinality;

    public HugePropertyKey(String name) {
        super(name);
        this.dataType = DataType.TEXT;
        this.cardinality = Cardinality.SINGLE;
    }

    @Override
    public HugePropertyKey indexNames(String... names) {
        this.indexNames.addAll(Arrays.asList(names));
        return this;
    }

    @Override
    public DataType dataType() {
        return this.dataType;
    }

    public void dataType(DataType dataType) {
        this.dataType = dataType;
    }

    @Override
    public Cardinality cardinality() {
        return this.cardinality;
    }

    public void cardinality(Cardinality cardinality) {
        this.cardinality = cardinality;
    }

    @Override
    public PropertyKey asText() {
        this.dataType(DataType.TEXT);
        return this;
    }

    @Override
    public PropertyKey asInt() {
        this.dataType(DataType.INT);
        return this;
    }

    @Override
    public PropertyKey asTimestamp() {
        this.dataType(DataType.TIMESTAMP);
        return this;
    }

    @Override
    public PropertyKey asUuid() {
        this.dataType(DataType.UUID);
        return this;
    }

    @Override
    public PropertyKey asBoolean() {
        this.dataType(DataType.BOOLEAN);
        return this;
    }

    @Override
    public PropertyKey asByte() {
        this.dataType(DataType.BYTE);
        return this;
    }

    @Override
    public PropertyKey asBlob() {
        this.dataType(DataType.BLOB);
        return this;
    }

    @Override
    public PropertyKey asDouble() {
        this.dataType(DataType.DOUBLE);
        return this;
    }

    @Override
    public PropertyKey asFloat() {
        this.dataType(DataType.FLOAT);
        return this;
    }

    @Override
    public PropertyKey asLong() {
        this.dataType(DataType.LONG);
        return this;
    }

    @Override
    public PropertyKey valueSingle() {
        this.cardinality(Cardinality.SINGLE);
        return this;
    }

    @Override
    public PropertyKey valueList() {
        this.cardinality(Cardinality.LIST);
        return this;
    }

    @Override
    public PropertyKey valueSet() {
        this.cardinality(Cardinality.SET);
        return this;
    }

    @Override
    public String schema() {
        String schema = "schema.propertyKey(\"" + this.name + "\")"
                + "." + this.dataType.schema() + "()"
                + "." + this.cardinality.string() + "()"
                + "." + propertiesSchema()
                + ".create();";
        return schema;
    }

    @Override
    public HugePropertyKey create() {

        StringUtil.verifyName(this.name);
        // Try to read
        PropertyKey propertyKey = this.transaction().getPropertyKey(this.name);
        // if propertyKey exist and checkExits
        if (propertyKey != null && checkExits) {
            throw new HugeException("The property key: " + this.name + " has exist");
        }

        StringUtil.verifyName(this.name);
        this.transaction().addPropertyKey(this);
        return this;
    }

    @Override
    public void remove() {
        this.transaction().removePropertyKey(this.name);
    }

}
