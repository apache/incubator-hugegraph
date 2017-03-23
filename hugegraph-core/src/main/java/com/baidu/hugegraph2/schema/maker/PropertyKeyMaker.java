package com.baidu.hugegraph2.schema.maker;

/**
 * Created by jishilei on 17/3/17.
 * 这一层接口是否可以去掉呢？
 */
public interface PropertyKeyMaker extends SchemaMaker {

    public PropertyKeyMaker asText();

    public PropertyKeyMaker asInt();

    public PropertyKeyMaker asTimeStamp();

    public PropertyKeyMaker asUUID();

    public PropertyKeyMaker single();

    public PropertyKeyMaker multiple();

    public PropertyKeyMaker properties(String... propertyName);
}
