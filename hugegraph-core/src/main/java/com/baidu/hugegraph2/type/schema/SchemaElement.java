package com.baidu.hugegraph2.type.schema;

import java.util.Map;
import java.util.Set;

import com.baidu.hugegraph2.backend.tx.SchemaTransaction;
import com.baidu.hugegraph2.type.HugeType;
import com.baidu.hugegraph2.type.Namifiable;

/**
 * Created by jishilei on 17/3/17.
 */
public interface SchemaElement extends Namifiable, HugeType {

    // schema description
    public String schema();

    public SchemaElement properties(String... propertyNames);

    public void create();

    public void remove();
}
