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

import java.text.ParseException;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.baidu.hugegraph.date.SafeDateFormat;
import com.google.common.collect.ImmutableMap;

public final class DateUtil {

    private static final Map<String, String> VALID_DFS = ImmutableMap.of(
            "^\\d{4}-\\d{1,2}-\\d{1,2}",
            "yyyy-MM-dd",
            "^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{2}:\\d{2}:\\d{2}",
            "yyyy-MM-dd HH:mm:ss",
            "^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{2}:\\d{2}:\\d{2}\\.\\d{3}",
            "yyyy-MM-dd HH:mm:ss.SSS"
    );

    private static final Map<String, SafeDateFormat> DATE_FORMATS =
                                                     new ConcurrentHashMap<>();

    public static Date parse(String value) {
        for (Map.Entry<String, String> entry : VALID_DFS.entrySet()) {
            if (value.matches(entry.getKey())) {
                try {
                    return parse(value, entry.getValue());
                } catch (ParseException e) {
                    throw new IllegalArgumentException(String.format(
                              "%s, expect format: %s",
                              e.getMessage(), entry.getValue()));
                }
            }
        }
        throw new IllegalArgumentException(String.format(
                  "Expect date formats are: %s, but got '%s'",
                  VALID_DFS.values(), value));
    }

    public static Date parse(String value, String df) throws ParseException {
        SafeDateFormat dateFormat = getDateFormat(df);
        return dateFormat.parse(value);
    }

    private static SafeDateFormat getDateFormat(String df) {
        SafeDateFormat dateFormat = DATE_FORMATS.get(df);
        if (dateFormat == null) {
            dateFormat = new SafeDateFormat(df);
            /*
             * Specify whether or not date/time parsing is to be lenient.
             * With lenient parsing, the parser may use heuristics to interpret
             * inputs that do not precisely match this object's format.
             * With strict parsing, inputs must match this object's format.
             */
            dateFormat.setLenient(false);
            SafeDateFormat previous = DATE_FORMATS.putIfAbsent(df, dateFormat);
            if (previous != null) {
                dateFormat = previous;
            }
        }
        return dateFormat;
    }

    public static Object toPattern(String df) {
        SafeDateFormat dateFormat = getDateFormat(df);
        return dateFormat.toPattern();
    }
}
