package com.baidu.hugegraph2.schema.base.maker;

import com.baidu.hugegraph2.schema.base.SchemaType;

/**
 * Created by jishilei on 17/3/17.
 */
public interface SchemaMaker {

    /**
     * Returns the name of this configured relation type.
     *
     * @return
     */
    public String getName();

    public SchemaType create();

    public SchemaType add();

    public void remove();

}
