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

package com.baidu.hugegraph.unit.core;

import java.util.Map;

import org.junit.Test;

import com.baidu.hugegraph.exception.HugeGremlinException;
import com.baidu.hugegraph.testutil.Assert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class ExceptionTest {

    @Test
    public void testHugeGremlinException() {
        Map<String, Object> response = ImmutableMap.of(
                "message", "Not allowed to call System.exit() via Gremlin",
                "Exception-Class", "java.lang.SecurityException",
                "exceptions", ImmutableList.of("java.lang.SecurityException"),
                "stackTrace", ""
        );
        HugeGremlinException e = new HugeGremlinException(200, response);
        Assert.assertEquals(200, e.statusCode());
        Assert.assertEquals(response, e.response());
    }
}
