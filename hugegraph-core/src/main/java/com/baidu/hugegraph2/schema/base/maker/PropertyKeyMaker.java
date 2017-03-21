package com.baidu.hugegraph2.schema.base.maker;

/**
 * Created by jishilei on 17/3/17.
 */
public interface PropertyKeyMaker extends SchemaMaker {

    public PropertyKeyMaker asText();

    public PropertyKeyMaker asInt();

    public PropertyKeyMaker asTimeStamp();



    public PropertyKeyMaker single();
    public PropertyKeyMaker multiple();


    // 这个貌似很重要
    public PropertyKeyMaker properties(String propertyName);
}
