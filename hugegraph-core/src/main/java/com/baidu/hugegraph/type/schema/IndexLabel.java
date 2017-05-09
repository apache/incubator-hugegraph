package com.baidu.hugegraph.type.schema;

import java.util.Set;

import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.IndexType;

/**
 * Created by liningrui on 2017/4/21.
 */
public abstract class IndexLabel extends SchemaElement {

    public IndexLabel(String name, SchemaTransaction transaction) {
        super(name, transaction);
    }

    @Override
    public HugeType type() {
        return HugeType.INDEX_LABEL;
    }

    public abstract IndexLabel by(String... indexFields);

    public abstract IndexLabel secondary();

    public abstract IndexLabel search();

    public abstract void create();

    public abstract HugeType baseType();

    public abstract String baseValue();

    public abstract IndexType indexType();

    public abstract Set<String> indexFields();

}
