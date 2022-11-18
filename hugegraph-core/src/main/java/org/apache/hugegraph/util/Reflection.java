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

package org.apache.hugegraph.util;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.exception.NotSupportException;
import org.slf4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

public class Reflection {

    private static final Logger LOG = Log.logger(Reflection.class);

    private static final Class<?> REFLECTION_CLAZZ;
    private static final Method REGISTER_FILEDS_TO_FILTER_METHOD;
    private static final Method REGISTER_METHODS_TO_FILTER_MOTHOD;

    public static final String JDK_INTERNAL_REFLECT_REFLECTION =
                               "jdk.internal.reflect.Reflection";
    public static final String SUN_REFLECT_REFLECTION =
                               "sun.reflect.Reflection";

    static {
        Method registerFieldsToFilterMethodTemp = null;
        Method registerMethodsToFilterMethodTemp = null;
        Class<?> reflectionClazzTemp = null;
        try {
            reflectionClazzTemp = Class.forName(
                                  JDK_INTERNAL_REFLECT_REFLECTION);
        } catch (ClassNotFoundException e) {
            try {
                reflectionClazzTemp = Class.forName(SUN_REFLECT_REFLECTION);
            } catch (ClassNotFoundException ex) {
                LOG.error("Can't find Reflection class", ex);
            }
        }

        REFLECTION_CLAZZ = reflectionClazzTemp;

        if (REFLECTION_CLAZZ != null) {
            try {
                registerFieldsToFilterMethodTemp =
                        REFLECTION_CLAZZ.getMethod("registerFieldsToFilter",
                                                   Class.class, String[].class);
            } catch (Throwable e) {
                LOG.error("Can't find registerFieldsToFilter method", e);
            }

            try {
                registerMethodsToFilterMethodTemp =
                        REFLECTION_CLAZZ.getMethod("registerMethodsToFilter",
                                                   Class.class, String[].class);
            } catch (NoSuchMethodException e) {
                LOG.error("Can't find registerMethodsToFilter method", e);
            }
        }
        REGISTER_FILEDS_TO_FILTER_METHOD = registerFieldsToFilterMethodTemp;
        REGISTER_METHODS_TO_FILTER_MOTHOD = registerMethodsToFilterMethodTemp;
    }

    public static void registerFieldsToFilter(Class<?> containingClass,
                                              String... fieldNames) {
        if (REGISTER_FILEDS_TO_FILTER_METHOD == null) {
            throw new NotSupportException(
                      "Reflection.registerFieldsToFilter()");
        }

        try {
            REGISTER_FILEDS_TO_FILTER_METHOD.setAccessible(true);
            REGISTER_FILEDS_TO_FILTER_METHOD.invoke(REFLECTION_CLAZZ,
                                                    containingClass, fieldNames);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new HugeException(
                      "Failed to register class '%s' fields to filter: %s",
                       containingClass, Arrays.toString(fieldNames));
        }
    }

    public static void registerMethodsToFilter(Class<?> containingClass,
                                               String... methodNames) {
        if (REGISTER_METHODS_TO_FILTER_MOTHOD == null) {
            throw new NotSupportException(
                      "Reflection.registerMethodsToFilterMethod()");
        }

        try {
            REGISTER_METHODS_TO_FILTER_MOTHOD.setAccessible(true);
            REGISTER_METHODS_TO_FILTER_MOTHOD.invoke(REFLECTION_CLAZZ,
                                                     containingClass, methodNames);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new HugeException(
                      "Failed to register class '%s' methods to filter: %s",
                      containingClass, Arrays.toString(methodNames));
        }
    }
}
