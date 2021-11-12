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

package com.baidu.hugegraph.traversal.optimize;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;

public class Text {

    private static final String START_SYMBOL = "(";
    private static final String END_SYMBOL = ")";

    public static ConditionP contains(String value) {
        // Support entire word match Text.contains("(word)")
        if (value.startsWith(START_SYMBOL) && value.endsWith(END_SYMBOL)) {
            String sting = value.substring(1, value.length() - 1);
            return ConditionP.textContainsEnhance(sting);
        }
        return ConditionP.textContains(value);
    }

    public static Id uuid(String id) {
        return IdGenerator.of(id, true);
    }
}
