package com.baidu.hugegraph2.schema.base.maker;

/**
 * Created by jishilei on 17/3/17.
 */
public interface SchemaManager {

    public PropertyKeyMaker propertyKey(String name);

    public VertexLabelMaker vertexLabel(String name);

    public EdgeLabelMaker edgeLabel(String name);

    public void desc();

}
