/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.type.define;

import java.util.Date;
import java.util.UUID;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.util.DateUtil;

public enum DataType implements SerialEnum {

    // This property has sub properties
    OBJECT(1, "object", Object.class),
    BOOLEAN(2, "boolean", Boolean.class),
    BYTE(3, "byte", Byte.class),
    INT(4, "int", Integer.class),
    LONG(5, "long", Long.class),
    FLOAT(6, "float", Float.class),
    DOUBLE(7, "double", Double.class),
    TEXT(8, "text", String.class),
    BLOB(9, "blob", byte[].class),
    DATE(10, "date", Date.class),
    UUID(11, "uuid", UUID.class);

    private byte code = 0;
    private String name = null;
    private Class<?> clazz = null;

    static {
        SerialEnum.register(DataType.class);
    }

    DataType(int code, String name, Class<?> clazz) {
        assert code < 256;
        this.code = (byte) code;
        this.name = name;
        this.clazz = clazz;
    }

    @Override
    public byte code() {
        return this.code;
    }

    public String string() {
        return this.name;
    }

    public Class<?> clazz() {
        return this.clazz;
    }

    public boolean isText() {
        return this == DataType.TEXT;
    }

    public boolean isNumber() {
        return this == BYTE || this == INT || this == LONG ||
               this == FLOAT || this == DOUBLE;
    }

    public boolean isNumber4() {
        // Store index value of Byte using 4 bytes
        return this == BYTE || this == INT || this == FLOAT;
    }

    public boolean isNumber8() {
        return this == LONG || this == DOUBLE;
    }

    public boolean isDate() {
        return this == DataType.DATE;
    }

    public boolean isUUID() {
        return this == DataType.UUID;
    }

    public <V> Number valueToNumber(V value) {
        if (!(this.isNumber() && value instanceof Number)) {
            return null;
        }
        if (this.clazz.isInstance(value)) {
            return (Number) value;
        }

        Number number = null;
        try {
            switch (this) {
                case BYTE:
                    number = Byte.valueOf(value.toString());
                    break;
                case INT:
                    number = Integer.valueOf(value.toString());
                    break;
                case LONG:
                    number = Long.valueOf(value.toString());
                    break;
                case FLOAT:
                    Float fvalue = Float.valueOf(value.toString());
                    if (!fvalue.isInfinite() && !fvalue.isNaN()) {
                        number = fvalue;
                    }
                    break;
                case DOUBLE:
                    Double dvalue = Double.valueOf(value.toString());
                    if (!dvalue.isInfinite() && !dvalue.isNaN()) {
                        number = dvalue;
                    }
                    break;
                default:
                    throw new AssertionError(String.format(
                              "Number type only contains Byte, Integer, " +
                              "Long, Float, Double, but got %s", this.clazz()));
            }
        } catch (NumberFormatException ignored) {
            // Unmatched type found
        }
        return number;
    }

    public <V> Date valueToDate(V value) {
        if (!this.isDate()) {
            return null;
        }
        if (value instanceof Date) {
            return (Date) value;
        } else if (value instanceof Number) {
            return new Date(((Number) value).longValue());
        } else if (value instanceof String) {
            return DateUtil.parse((String) value);
        }
        return null;
    }

    public <V> UUID valueToUUID(V value) {
        if (!this.isUUID()) {
            return null;
        }
        if (value instanceof UUID) {
            return (UUID) value;
        } else if (value instanceof String) {
            return java.util.UUID.fromString((String) value);
        }
        return null;
    }

    public static DataType fromClass(Class<?> clazz) {
        for (DataType type : DataType.values()) {
            if (type.clazz() == clazz) {
                return type;
            }
        }
        throw new HugeException("Unknow clazz '%s' for DataType", clazz);
    }
}
