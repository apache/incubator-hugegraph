package com.baidu.hugegraph2.schema.base.maker;

/**
 * Created by jishilei on 17/3/17.
 */
public interface VertexLabelMaker extends SchemaMaker {

    public VertexLabelMaker index(String byName);

    public VertexLabelMaker secondary();

    public VertexLabelMaker materialized();

    public VertexLabelMaker by(String name);
}
