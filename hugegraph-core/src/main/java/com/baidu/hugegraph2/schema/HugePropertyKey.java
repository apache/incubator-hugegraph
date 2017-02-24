package com.baidu.hugegraph2.schema;

import com.baidu.hugegraph2.Cardinality;
import com.baidu.hugegraph2.DataType;
import com.baidu.hugegraph2.schema.base.PropertyKey;

/**
 * Created by jishilei on 17/3/17.
 */
public class HugePropertyKey implements PropertyKey {

    private DataType dataType;
    private Cardinality cardinality;
    private String name;

    public HugePropertyKey(String name) {
        this.dataType = DataType.OBJECT;
        this.cardinality = Cardinality.SINGLE;
        this.name = name;
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    @Override
    public Cardinality cardinality() {
        return this.cardinality;
    }

    public void setCardinality(Cardinality cardinality) {
        this.cardinality = cardinality;
    }

    public void setDataType(DataType dataType) {

        this.dataType = dataType;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String toString() {

        return "name = " + name
                + " , dataType=" + dataType.toString()
                + " , cardinality=" + cardinality.toString();
    }

    @Override
    public String schema() {
        return "schema.propertyKey(\"" + name + "\")"
                + "." + cardinality.toString() + "()"
                + "." + dataType.toString() + "()"
                + ".create();";
    }
}
