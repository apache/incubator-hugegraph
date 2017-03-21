package com.baidu.hugegraph2.schema.base.maker;

/**
 * Created by jishilei on 17/3/17.
 */
public interface EdgeLabelMaker extends SchemaMaker {

    public EdgeLabelMaker connection(String fromVertexLabel, String toVertexLabel);

    public EdgeLabelMaker single();
    public EdgeLabelMaker multiple();

    public EdgeLabelMaker partitionKeys(String... keys);

    public EdgeLabelMaker inMany2One();
    public EdgeLabelMaker inMany2Many();
    public EdgeLabelMaker inOne2Many();
    public EdgeLabelMaker inOne2One();
}
