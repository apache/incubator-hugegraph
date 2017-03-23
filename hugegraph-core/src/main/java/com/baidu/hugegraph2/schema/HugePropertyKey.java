package com.baidu.hugegraph2.schema;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.baidu.hugegraph2.Cardinality;
import com.baidu.hugegraph2.DataType;
import com.baidu.hugegraph2.type.schema.PropertyKey;
import com.baidu.hugegraph2.util.StringUtil;

/**
 * Created by jishilei on 17/3/17.
 */
public class HugePropertyKey implements PropertyKey {

    private String name;
    private DataType dataType;
    private Cardinality cardinality;
    // propertykey可能还有properties
    private Set<String> properties;


    public HugePropertyKey(String name) {
        this.name = name;
        this.dataType = DataType.OBJECT;
        this.cardinality = Cardinality.SINGLE;
        this.properties = null;
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    public void dataType(DataType dataType) {
        this.dataType = dataType;
    }

    @Override
    public Cardinality cardinality() {
        return cardinality;
    }

    public void cardinality(Cardinality cardinality) {
        this.cardinality = cardinality;
    }

    @Override
    public Set<String> properties() {
        return properties;
    }

    public void properties(String... propertyNames) {
        if (properties == null) {
            properties = new HashSet<>();
        }
        properties.addAll(Arrays.asList(propertyNames));
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String toString() {
        return String.format("{name=%s, dataType=%s, cardinality=%s}",
                name, dataType.toString(), cardinality.toString());
    }

    @Override
    public String schema() {
        return "schema.propertyKey(\"" + name + "\")"
                + "." + cardinality.toString().toLowerCase() + "()"
                + ".as" + StringUtil.captureName(dataType.toString().toLowerCase()) + "()"
                + ".create();";
    }
}
