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

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.exception.NotSupportException;
import org.slf4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

public class Reflection {

    private static final Logger LOG = Log.logger(Reflection.class);

    private static final Class reflectionClazz;
    private static final Method registerFieldsToFilterMethod;
    private static final Method registerMethodsToFilterMethod;

    public static final String JDK_INTERNAL_REFLECT_REFLECTION =
                               "jdk.internal.reflect.Reflection";
    public static final String SUN_REFLECT_REFLECTION =
                               "sun.reflect.Reflection";

    static {
        Method registerFieldsToFilterMethodTemp = null;
        Method registerMethodsToFilterMethodTemp = null;
        Class reflectionClazzTemp = null;
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

        reflectionClazz = reflectionClazzTemp;

        if (reflectionClazz != null) {
            try {
                registerFieldsToFilterMethodTemp =
                reflectionClazz.getMethod("registerFieldsToFilter",
                                          Class.class, String[].class);
            } catch (Throwable e) {
                LOG.error("Can't find registerFieldsToFilter method", e);
            }

            try {
                registerMethodsToFilterMethodTemp =
                reflectionClazz.getMethod("registerMethodsToFilter",
                                          Class.class, String[].class);
            } catch (NoSuchMethodException e) {
                LOG.error("Can't find registerMethodsToFilter method", e);
            }
        }
        registerFieldsToFilterMethod = registerFieldsToFilterMethodTemp;
        registerMethodsToFilterMethod = registerMethodsToFilterMethodTemp;
    }

    public static void registerFieldsToFilter(Class<?> containingClass,
                                              String ... fieldNames) {
        if (registerFieldsToFilterMethod == null) {
            throw new NotSupportException(
                      "No support this method 'registerFieldsToFilter'");
        }

        try {
            registerFieldsToFilterMethod.setAccessible(true);
            registerFieldsToFilterMethod.invoke(
            reflectionClazz, containingClass, fieldNames);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new HugeException(
                      "Register class '{}' filter fields '{}' is failed",
                       containingClass, Arrays.toString(fieldNames));
        }
    }

    public static void registerMethodsToFilter(Class<?> containingClass,
                                               String ... methodNames) {
        if (registerMethodsToFilterMethod == null) {
            throw new NotSupportException(
                      "Currently Java version no support " +
                      "the method 'registerMethodsToFilterMethod'");
        }

        try {
            registerMethodsToFilterMethod.setAccessible(true);
            registerMethodsToFilterMethod.invoke(
            reflectionClazz, containingClass, methodNames);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new HugeException(
                      "Register class '{}' filter method '{}' is failed",
                       containingClass, Arrays.toString(methodNames));
        }
    }
}
