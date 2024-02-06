/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.traversal.algorithm.records.record;

import org.apache.hugegraph.type.define.SerialEnum;

public enum RecordType implements SerialEnum {

    // One key with one int value
    INT(1, "int"),

    // One key with multi unique values
    SET(2, "set"),

    // One key with multi values
    ARRAY(3, "array");

    private final byte code;
    private final String name;

    static {
        SerialEnum.register(RecordType.class);
    }

    RecordType(int code, String name) {
        assert code < 256;
        this.code = (byte) code;
        this.name = name;
    }

    @Override
    public byte code() {
        return this.code;
    }

    public String string() {
        return this.name;
    }

    public static RecordType fromCode(byte code) {
        switch (code) {
            case 1:
                return INT;
            case 2:
                return SET;
            case 3:
                return ARRAY;
            default:
                throw new AssertionError("Unsupported record code: " + code);
        }
    }
}
