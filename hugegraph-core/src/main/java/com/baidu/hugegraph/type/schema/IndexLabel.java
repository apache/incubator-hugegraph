package com.baidu.hugegraph.type.schema;

import java.util.List;

import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.IndexType;

/**
 * Created by liningrui on 2017/4/21.
 */
public abstract class IndexLabel extends SchemaElement {

    public IndexLabel(String name) {
        super(name);
    }

    @Override
    public HugeType type() {
        return HugeType.INDEX_LABEL;
    }

    public abstract IndexLabel on(SchemaElement element);

    public abstract IndexLabel by(String... indexFields);

    public abstract IndexLabel secondary();

    public abstract IndexLabel search();

    @Override
    public IndexLabel ifNotExist() {
        this.checkExits = false;
        return this;
    }

    @Override
    public abstract IndexLabel create();

    public abstract HugeType baseType();

    public abstract String baseValue();

    public abstract IndexType indexType();

    public abstract List<String> indexFields();
}
