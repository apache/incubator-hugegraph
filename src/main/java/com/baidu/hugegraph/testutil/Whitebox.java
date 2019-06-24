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

package com.baidu.hugegraph.testutil;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Objects;

import com.baidu.hugegraph.util.E;
import com.google.common.primitives.Primitives;

public class Whitebox {

    public static final char SEPARATOR = '.';

    public static void setInternalState(Object target, String fieldName,
                                        Object value) {
        assert target != null;
        assert fieldName != null;
        int sep = fieldName.lastIndexOf(SEPARATOR);
        if (sep > 0) {
            target = getInternalState(target, fieldName.substring(0, sep));
            fieldName = fieldName.substring(sep + 1);
        }

        Class<?> c = target instanceof Class<?> ?
                     (Class<?>) target : target.getClass();
        try {
            Field f = getFieldFromHierarchy(c, fieldName);
            f.setAccessible(true);
            f.set(target, value);
        } catch (Exception e) {
            throw new RuntimeException(String.format(
                      "Can't set value of '%s' against object '%s'",
                      fieldName, target), e);
        }
    }

    public static <T> T getInternalState(Object target, String fieldName) {
        assert fieldName != null;
        int sep = fieldName.indexOf(SEPARATOR);
        if (sep > 0) {
            String field = fieldName.substring(0, sep);
            Object value = getInternalState(target, field);
            field = fieldName.substring(sep + 1);
            return getInternalState(value, field);
        }

        Class<?> c = target instanceof Class<?> ?
                     (Class<?>) target : target.getClass();
        try {
            Field f = getFieldFromHierarchy(c, fieldName);
            f.setAccessible(true);
            @SuppressWarnings("unchecked")
            T result = (T) f.get(target);
            return result;
        } catch (Exception e) {
            throw new RuntimeException(String.format(
                      "Unable to get internal state on field '%s' of %s",
                      fieldName, target), e);
        }
    }

    private static Field getFieldFromHierarchy(Class<?> clazz, String field) {
        Field f = getField(clazz, field);
        while (f == null && clazz != Object.class) {
            clazz = clazz.getSuperclass();
            f = getField(clazz, field);
        }
        if (f == null) {
            throw new RuntimeException(String.format(
                      "Not declared field '%s' in class '%s'",
                      field, clazz.getSimpleName()));
        }
        return f;
    }

    private static Field getField(Class<?> clazz, String field) {
        try {
            return clazz.getDeclaredField(field);
        } catch (NoSuchFieldException e) {
            return null;
        }
    }

    public static <T> T invokeStatic(Class<?> clazz, String methodName,
                                     Object... args) {
        return invoke(clazz, methodName, (Object) null, args);
    }

    public static <T> T invokeStatic(Class<?> clazz,  Class<?>[] classes,
                                     String methodName, Object... args) {
        return invoke(clazz, classes, methodName, (Object) null, args);
    }

    public static <T> T invoke(Object owner, String field,
                               String methodName, Object... args) {
        Object self = getInternalState(owner, field);
        Objects.requireNonNull(self);
        return invoke(self.getClass(), methodName, self, args);
    }

    public static <T> T invoke(Object owner, String field, Class<?>[] classes,
                               String methodName, Object... args) {
        Object self = getInternalState(owner, field);
        Objects.requireNonNull(self);
        return invoke(self.getClass(), classes, methodName, self, args);
    }

    public static <T> T invoke(Class<?> clazz, String methodName,
                               Object self, Object... args) {
        Class<?>[] classes = new Class<?>[args.length];
        int i = 0;
        for (Object arg : args) {
            E.checkArgument(arg != null, "The argument can't be null");
            classes[i++] = Primitives.unwrap(arg.getClass());
        }
        return invoke(clazz, classes, methodName, self, args);
    }

    public static <T> T invoke(Class<?> clazz, Class<?>[] classes,
                               String methodName, Object self, Object... args) {
        try {
            Method method = clazz.getDeclaredMethod(methodName, classes);
            method.setAccessible(true);
            @SuppressWarnings("unchecked")
            T result = (T) method.invoke(self, args);
            return result;
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(String.format(
                      "Can't find method '%s' with args %s of class '%s'",
                      methodName, Arrays.asList(classes), clazz), e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(String.format(
                      "Can't invoke method '%s' of class '%s': %s",
                      methodName, clazz, e.getMessage()), e);
        } catch (InvocationTargetException e) {
            Throwable target = e.getTargetException();
            if (target instanceof RuntimeException) {
                throw (RuntimeException) target;
            }
            throw new RuntimeException(String.format(
                      "Can't invoke method '%s' of class '%s': %s",
                      methodName, clazz, target.getMessage()), target);
        }
    }
}
