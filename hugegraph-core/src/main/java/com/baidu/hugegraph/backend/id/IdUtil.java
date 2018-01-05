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

import com.baidu.hugegraph.util.E;

public class IdUtil {

    private static final String NUMBER_PREFIX = "L";
    private static final String STRING_PREFIX = "S";

    public static String writeString(Id id) {
        return (id.number() ? NUMBER_PREFIX : STRING_PREFIX) + id.asObject();
    }

    public static Id readString(String id) {
        E.checkNotNull(id, "id");
        String signal = id.substring(0, 1);
        E.checkState(signal.equals(NUMBER_PREFIX) ||
                     signal.equals(STRING_PREFIX),
                     "The serialized id value must start with '%s' or '%s', " +
                     "but got '%s'", NUMBER_PREFIX, STRING_PREFIX, id);
        id = id.substring(1);
        boolean number = signal.equals(NUMBER_PREFIX);
        if (number) {
            return IdGenerator.of(Long.valueOf(id));
        } else {
            return IdGenerator.of(id);
        }
    }
}
