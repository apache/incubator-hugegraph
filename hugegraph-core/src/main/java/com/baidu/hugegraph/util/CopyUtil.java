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
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.baidu.hugegraph.util;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import com.baidu.hugegraph.HugeException;

public class CopyUtil {

    public static boolean isSimpleType(Class<?> type) {
        if (type.isPrimitive() ||
            type.equals(String.class) ||
            type.equals(Boolean.class) ||
            NumericUtil.isNumber(type)) {
            return true;
        }

        return false;
    }

    @SuppressWarnings("unchecked")
    public static <T> T cloneObject(T o, T clone) throws Exception {
        if (clone == null) {
            clone = (T) o.getClass().newInstance();
        }
        for (Field field : o.getClass().getDeclaredFields()) {
            field.setAccessible(true);
            Object childObj = field.get(o);
            if (childObj == null || Modifier.isFinal(field.getModifiers())) {
                continue;
            }

            Class<?> declareType = field.getType();
            Class<?> valueType = childObj.getClass();
            if (isSimpleType(declareType) || isSimpleType(valueType)) {
                field.set(clone, field.get(o));
            } else {
                if (childObj == o) {
                    field.set(clone, clone);
                } else {
                    field.set(clone, cloneObject(field.get(o), null));
                }
            }
        }
        return clone;
    }

    public static <T> T copy(T object) {
        return copy(object, null);
    }

    public static <T> T copy(T object, T clone) {
        try {
            return cloneObject(object, clone);
        } catch (Exception e) {
            throw new HugeException("Failed to clone object", e);
        }
    }

    public static <T> T deepCopy(T object) {
        @SuppressWarnings("unchecked")
        Class<T> cls = (Class<T>) object.getClass();
        return JsonUtil.fromJson(JsonUtil.toJson(object), cls);
    }
}
