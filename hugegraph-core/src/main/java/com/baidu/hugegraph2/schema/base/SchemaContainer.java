package com.baidu.hugegraph2.schema.base;

import java.util.List;

import com.baidu.hugegraph2.schema.HugePropertyKey;

/**
 * Created by jishilei on 17/3/20.
 */
public interface SchemaContainer {
    public List<HugePropertyKey> getPropertyKeys();
}
