package com.baidu.hugegraph2.type.define;

import org.apache.commons.lang.StringUtils;

/**
 * Created by jishilei on 17/3/18.
 */
public enum DataType {
    OBJECT,
    TEXT,
    INT,
    UUID,
    TIMESTAMP;

    public String schema() {
        // enum object -> string -> lowercase -> capture name
        return "as" + StringUtils.capitalize(this.toString().toLowerCase());
    }
}
