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

package com.baidu.hugegraph.util;

import java.lang.reflect.Method;
import java.util.Collection;

import org.apache.commons.lang3.StringUtils;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.backend.id.Id;

public final class StringUtil {

    public static String desc(String prefix, Collection<Id> elems) {
        StringBuilder sb = new StringBuilder();
        for (Id elem : elems) {
            sb.append("\"").append(elem.asString()).append("\",");
        }
        int endIdx = sb.lastIndexOf(",") > 0 ? sb.length() - 1 : sb.length();
        return String.format(".%s(%s)", prefix, sb.substring(0, endIdx));
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
            parts[i] = StringUtils.replace(parts[i], escape + splitor, splitor);
        }
        return parts;
    }

    public static Object valueOf(Class<? extends Object> clazz, String number) {
        try {
            Method valueOf = clazz.getMethod("valueOf", String.class);
            return valueOf.invoke(null, number);
        } catch (NoSuchMethodException | SecurityException e) {
            throw new HugeException("Invalid number class: %s", e, clazz);
        } catch (Exception e) {
            throw new HugeException("Invalid value '%s' for number class: %s",
                                    e, number, clazz);
        }
    }
}
