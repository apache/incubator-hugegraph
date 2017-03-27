package com.baidu.hugegraph2.type.schema;

import com.baidu.hugegraph2.type.HugeType;
import com.baidu.hugegraph2.type.Namifiable;

/**
 * Created by jishilei on 17/3/17.
 */
public interface SchemaType extends Namifiable, HugeType {

    // schema description
    public String schema();

    public SchemaType properties(String... propertyNames);

    public void create();

    public void remove();
}
