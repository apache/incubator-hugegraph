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

package com.baidu.hugegraph.backend.id;

import org.apache.commons.lang3.StringUtils;

import com.baidu.hugegraph.backend.id.Id.IdType;

public final class IdUtil {

    public static String writeStoredString(Id id) {
        String string;
        switch (id.type()) {
            case LONG:
            case STRING:
            case UUID:
                string = IdGenerator.asStoredString(id);
                break;
            case EDGE:
                string = EdgeId.asStoredString(id);
                break;
            default:
                throw new AssertionError("Invalid id type " + id.type());
        }
        return id.type().prefix() + string;
    }

    public static Id readStoredString(String id) {
        IdType type = IdType.valueOfPrefix(id);
        id = id.substring(1);
        switch (type) {
            case LONG:
            case STRING:
            case UUID:
                return IdGenerator.ofStoredString(id, type);
            case EDGE:
                return EdgeId.parseStoredString(id);
            default:
                throw new AssertionError("Invalid id type " + type);
        }
    }

    public static String writeString(Id id) {
        return "" + id.type().prefix() + id.asObject();
    }

    public static Id readString(String id) {
        IdType type = IdType.valueOfPrefix(id);
        id = id.substring(1);
        switch (type) {
            case LONG:
                return IdGenerator.of(Long.parseLong(id));
            case STRING:
            case UUID:
                return IdGenerator.of(id, type == IdType.UUID);
            case EDGE:
                return EdgeId.parse(id);
            default:
                throw new AssertionError("Invalid id type " + type);
        }
    }

    public static String writeLong(Id id) {
        return String.valueOf(id.asLong());
    }

    public static Id readLong(String id) {
        return IdGenerator.of(Long.parseLong(id));
    }

    public static String escape(char splitor, char escape, String... values) {
        StringBuilder escaped = new StringBuilder((values.length + 1) << 4);
        // Do escape for every item in values
        for (String value : values) {
            if (escaped.length() > 0) {
                escaped.append(splitor);
            }

            if (value.indexOf(splitor) == -1) {
                escaped.append(value);
                continue;
            }

            // Do escape for current item
            for (int i = 0, n = value.length(); i < n; i++) {
                char ch = value.charAt(i);
                if (ch == splitor) {
                    escaped.append(escape);
                }
                escaped.append(ch);
            }
        }
        return escaped.toString();
    }

    public static String[] unescape(String id, String splitor, String escape) {
        /*
         * Note that the `splitor`/`escape` maybe special characters in regular
         * expressions, but this is a frequently called method, for faster
         * execution, we forbid the use of special characters as delimiter
         * or escape sign.
         * The `limit` param -1 in split method can ensure empty string be
         * splited to a part.
         */
        String[] parts = id.split("(?<!" + escape + ")" + splitor, -1);
        for (int i = 0; i < parts.length; i++) {
            parts[i] = StringUtils.replace(parts[i], escape + splitor,
                                           splitor);
        }
        return parts;
    }
}
