package com.baidu.hugegraph2.schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph2.HugeException;
import com.baidu.hugegraph2.backend.tx.SchemaTransaction;
import com.baidu.hugegraph2.type.define.Cardinality;
import com.baidu.hugegraph2.type.define.DataType;
import com.baidu.hugegraph2.type.schema.PropertyKey;
import com.baidu.hugegraph2.util.StringUtil;

/**
 * Created by jishilei on 17/3/17.
 */
public class HugePropertyKey extends PropertyKey {

    private static final Logger logger = LoggerFactory.getLogger(HugePropertyKey.class);

    private DataType dataType;
    private Cardinality cardinality;

    public HugePropertyKey(String name, SchemaTransaction transaction) {
        super(name, transaction);
        this.dataType = DataType.TEXT;
        this.cardinality = Cardinality.SINGLE;
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
    public String toString() {
        return String.format("{name=%s, dataType=%s, cardinality=%s}",
                this.name, this.dataType.toString(), this.cardinality.toString());
    }

    @Override
    public String schema() {
        String schema = "schema.propertyKey(\"" + this.name + "\")"
                + "." + this.dataType.schema() + "()"
                + "." + this.cardinality.schema() + "()"
                + "." + propertiesSchema()
                + ".create();";
        return schema;
    }

    @Override
    public void create() {
        // Try to read, if exist throw an error
        if (this.transaction.getPropertyKey(this.name) != null) {
            throw new HugeException("The propertyKey:" + this.name + " has existed.");
        }

        // check name is valid
        StringUtil.verifyName(this.name);
        this.transaction.addPropertyKey(this);
        this.commit();
    }

    @Override
    public void remove() {
        this.transaction.removePropertyKey(this.name);
    }

}
