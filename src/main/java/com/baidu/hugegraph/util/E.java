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

import java.util.Collection;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

/**
 * Created by liningrui on 2017/5/26.
 */
public final class E {

    public static void checkNotNull(Object object, String elem) {
        Preconditions.checkNotNull(object, "The '%s' can't be null", elem);
    }

    public static void checkNotNull(Object object, String elem, String owner) {
        Preconditions.checkNotNull(object,
                                   "The '%s' of '%s' can't be null",
                                   elem, owner);
    }

    public static void checkNotEmpty(Collection<?> collection, String elem) {
        Preconditions.checkArgument(!collection.isEmpty(),
                                    "The '%s' can't be empty", elem);
    }

    public static void checkNotEmpty(Collection<?> collection,
                                     String elem,
                                     String owner) {
        Preconditions.checkArgument(!collection.isEmpty(),
                                    "The '%s' of '%s' can't be empty",
                                    elem, owner);
    }

    public static void checkArgument(boolean expression,
                                     @Nullable String message,
                                     @Nullable Object... args) {
        Preconditions.checkArgument(expression, message, args);
    }

    public static void checkArgumentNotNull(Object object,
                                            @Nullable String message,
                                            @Nullable Object... args) {
        Preconditions.checkArgument(object != null, message, args);
    }

    public static void checkState(boolean expression,
                                  @Nullable String message,
                                  @Nullable Object... args) {
        Preconditions.checkState(expression, message, args);
    }
}
