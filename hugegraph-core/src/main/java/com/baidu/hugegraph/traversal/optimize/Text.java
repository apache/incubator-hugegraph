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
import com.baidu.hugegraph.util.E;

import java.util.regex.Pattern;

public class Text {

    public static ConditionP startingWith(String value) {
        return ConditionP.textStartingWith(value);
    }

    public static ConditionP notStartingWith(String value) {
        return ConditionP.textNotStartingWith(value);
    }

    public static ConditionP endingWith(String value) {
        return ConditionP.textEndingWith(value);
    }

    public static ConditionP notEndingWith(String value) {
        return ConditionP.textNotEndingWith(value);
    }

    public static ConditionP contains(String value) {
        return ConditionP.textContains(value);
    }

    public static ConditionP containing(String value) {
        return ConditionP.textContains(value);
    }

    public static ConditionP notContaining(String value) {
        return ConditionP.textNotContains(value);
    }

    public static ConditionP matchRegex(String value) {
        return ConditionP.textMatchRegex(value);
    }

    public static ConditionP matchEditDistance(String value, int distance) {
        E.checkArgument(distance >= 0, "The distance should be >= 0");
        String prefix = String.valueOf(distance) + "#";
        return ConditionP.textMatchEditDistance(prefix + value);
    }

    public static Id uuid(String id) {
        return IdGenerator.of(id, true);
    }
}
