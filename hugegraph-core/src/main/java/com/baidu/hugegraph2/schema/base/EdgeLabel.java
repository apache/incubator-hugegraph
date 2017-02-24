package com.baidu.hugegraph2.schema.base;

import com.baidu.hugegraph2.Multiplicity;

/**
 * Created by jishilei on 17/3/18.
 */
public interface EdgeLabel extends SchemaType {

    public Multiplicity getMultiplicity();

    public boolean isDirected();

}
