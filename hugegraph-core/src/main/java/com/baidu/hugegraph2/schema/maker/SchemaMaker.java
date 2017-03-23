package com.baidu.hugegraph2.schema.maker;

import com.baidu.hugegraph2.type.schema.SchemaType;

/**
 * Created by jishilei on 17/3/17.
 */
public interface SchemaMaker {

    /**
     * Returns the name of this configured relation type.
     * @return
     */
    public String name();

    public SchemaType create();

    public SchemaType add();

    public void remove();

}
