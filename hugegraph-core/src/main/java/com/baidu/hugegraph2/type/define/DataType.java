package com.baidu.hugegraph2.type.define;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

/**
 * Created by jishilei on 17/3/18.
 */
public enum DataType {
    OBJECT(1, "object"),
    TEXT(2, "text"),
    INT(3, "int"),
    LONG(4, "long"),
    UUID(5, "uuid"),
    TIMESTAMP(6, "timestamp");

    private byte code = 0;
    private String name = null;

    private static final Map<Byte, DataType> ALL_CODE = new HashMap<>();
    private static final Map<String, DataType> ALL_NAME = new HashMap<>();

    static {
        for (DataType dataType : values()) {
            ALL_CODE.put(dataType.code, dataType);
            ALL_NAME.put(dataType.name, dataType);
        }
    }

    private DataType(int code, String name) {
        assert code < 256;
        this.code = (byte) code;
        this.name = name;
    }

    public String schema() {
        // enum object -> string -> lowercase -> capture name
        return "as" + StringUtils.capitalize(this.toString().toLowerCase());
    }

    public static DataType fromCode(byte dataType) {
        return ALL_CODE.get(dataType);
    }

    public static DataType fromString(String dataType) {
        return ALL_NAME.get(dataType);
    }

    public byte code() {
        return this.code;
    }

    public String string() {
        return this.name;
    }
}
