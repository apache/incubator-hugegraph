package com.baidu.hugegraph2.schema;

import java.util.List;

import com.baidu.hugegraph2.backend.tx.SchemaTransaction;
import com.baidu.hugegraph2.schema.base.SchemaContainer;

/**
 * Created by jishilei on 2017/3/22.
 */
public class HugeSchemaContainer implements SchemaContainer {

    private SchemaTransaction transaction;

    public HugeSchemaContainer(SchemaTransaction transaction) {
        this.transaction = transaction;
    }

    @Override
    public List<HugePropertyKey> getPropertyKeys() {
        return this.transaction.getPropertyKeys();
    }
}
