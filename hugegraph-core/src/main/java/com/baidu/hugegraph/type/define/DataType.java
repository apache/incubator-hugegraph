package com.baidu.hugegraph.type.define;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;

import com.baidu.hugegraph.structure.HugeProperty;

/**
 * Created by jishilei on 17/3/18.
 */
public enum DataType {

    // this property has sub properties
    OBJECT(1, "object", HugeProperty.class),
    BOOLEAN(2, "boolean", Boolean.class),
    BYTE(3, "byte", Byte.class),
    BLOB(4, "blob", byte[].class),
    DOUBLE(5, "double", Double.class),
    FLOAT(6, "float", Float.class),
    INT(7, "int", Integer.class),
    LONG(8, "long", Long.class),
    TEXT(9, "text", String.class),
    TIMESTAMP(10, "timestamp", Timestamp.class),
    UUID(11, "uuid", UUID.class);

    private byte code = 0;
    private String name = null;
    private Class<?> clazz = null;

    private static final Map<Byte, DataType> ALL_CODE = new HashMap<>();
    private static final Map<String, DataType> ALL_NAME = new HashMap<>();
    private static final Map<Class<?>, DataType> ALL_CLASS = new HashMap<>();

    static {
        for (DataType dataType : values()) {
            ALL_CODE.put(dataType.code, dataType);
            ALL_NAME.put(dataType.name, dataType);
            ALL_CLASS.put(dataType.clazz, dataType);
        }
    }

    private DataType(int code, String name, Class<?> clazz) {
        assert code < 256;
        this.code = (byte) code;
        this.name = name;
        this.clazz = clazz;
    }

    public String schema() {
        return String.format(".as%s()", StringUtils.capitalize(this.name));
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

    public Class<?> clazz() {
        return this.clazz;
    }
}
