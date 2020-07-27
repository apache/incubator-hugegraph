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

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.CollectionUtil;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

public interface SerialEnum {

    public byte code();

    static Table<Class<?>, Byte, SerialEnum> table = HashBasedTable.create();

    public static void register(Class<? extends SerialEnum> clazz) {
        Object enums;
        try {
            enums = clazz.getMethod("values").invoke(null);
        } catch (Exception e) {
            throw new BackendException(e);
        }
        for (SerialEnum e : CollectionUtil.<SerialEnum>toList(enums)) {
            table.put(clazz, e.code(), e);
        }
    }

    public static <T extends SerialEnum> T fromCode(Class<T> clazz, byte code) {
        @SuppressWarnings("unchecked")
        T value = (T) table.get(clazz, code);
        if (value == null) {
            E.checkArgument(false, "Can't construct %s from code %s",
                            clazz.getSimpleName(), code);
        }
        return value;
    }

    public static void registerInternalEnums() {
        SerialEnum.register(Action.class);
        SerialEnum.register(HugeType.class);
    }
}
