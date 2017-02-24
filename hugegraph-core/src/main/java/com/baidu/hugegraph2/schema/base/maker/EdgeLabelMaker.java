package com.baidu.hugegraph2.schema.base.maker;

/**
 * Created by jishilei on 17/3/17.
 */
public interface EdgeLabelMaker {

    public EdgeLabelMaker connection(String fromVertexLabel, String toVertexLabel);

    public EdgeLabelMaker multi();

    public EdgeLabelMaker simple();

}
