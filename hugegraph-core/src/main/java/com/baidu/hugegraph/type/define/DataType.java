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

public enum DataType implements SerialEnum {

    // This property has sub properties
    OBJECT(1, "object", Object.class),
    BOOLEAN(2, "boolean", Boolean.class),
    BYTE(3, "byte", Byte.class),
    BLOB(4, "blob", byte[].class),
    DOUBLE(5, "double", Double.class),
    FLOAT(6, "float", Float.class),
    INT(7, "int", Integer.class),
    LONG(8, "long", Long.class),
    TEXT(9, "text", String.class),
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

    public boolean isNumberType() {
        return this == BYTE || this == INT || this == LONG ||
               this == FLOAT || this == DOUBLE;
    }

    public <V> Number valueToNumber(V value) {
        if (!(this.isNumberType() && value instanceof Number)) {
            return null;
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
                    number = Float.valueOf(value.toString());
                    break;
                case DOUBLE:
                    number = Double.valueOf(value.toString());
                    break;
                default:
                    throw new AssertionError(String.format(
                              "Number type only contains Byte, Integer, " +
                              "Long, Float, Double, but got %s", this.clazz()));
            }
        } catch (NumberFormatException ignore) {
            // Unmatched type found
        }
        return number;
    }
}
