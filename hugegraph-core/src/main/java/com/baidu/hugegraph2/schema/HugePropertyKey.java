package com.baidu.hugegraph2.schema;

import java.util.HashSet;
import java.util.Set;

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

    // propertykey可能还有properties，这里应该要有一个怎样的变量呢
    private Set<PropertyKey> properties;


    public HugePropertyKey(String name) {
        this.dataType = DataType.OBJECT;
        this.cardinality = Cardinality.SINGLE;
        this.name = name;
        this.properties = null;
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

    /**
     * 给属性键再添加属性
     * @param propertyName
     */
    public void addProperties(String propertyName) {
        if (properties == null) {
            properties = new HashSet<>();
        }
        properties.add(new HugePropertyKey(propertyName));
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
