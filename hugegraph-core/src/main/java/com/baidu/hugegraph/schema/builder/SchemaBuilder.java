package com.baidu.hugegraph.schema.builder;

import com.baidu.hugegraph.schema.SchemaElement;

public interface SchemaBuilder {

    SchemaElement create();

    SchemaElement append();

    SchemaElement eliminate();

    void remove();
}
