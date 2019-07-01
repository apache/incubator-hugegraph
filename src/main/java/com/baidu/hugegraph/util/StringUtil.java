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

import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.List;

public final class StringUtil {

    public static List<CharSequence> split(String line, String delimiter) {
        E.checkArgument(delimiter.length() > 0,
                        "The delimiter can't be empty");
        List<CharSequence> results = new ArrayList<>();
        int from = 0;
        for (int to; (to = line.indexOf(delimiter, from)) >= 0;
             from = to + delimiter.length()) {
            results.add(CharBuffer.wrap(line, from, to));
        }
        results.add(CharBuffer.wrap(line, from, line.length()));
        return results;
    }
}
