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
        // 枚举对象 -> 字符串 -> 小写 -> 首字母大写
        return "as" + StringUtils.capitalize(this.toString().toLowerCase());
    }
}
