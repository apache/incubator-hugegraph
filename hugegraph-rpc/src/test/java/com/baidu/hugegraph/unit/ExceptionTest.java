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

package com.baidu.hugegraph.unit;

import org.junit.Test;

import com.baidu.hugegraph.rpc.RpcException;
import com.baidu.hugegraph.testutil.Assert;

public class ExceptionTest {

    @Test
    public void testExceptionWithMessage() {
        RpcException e = new RpcException("test");
        Assert.assertEquals("test", e.getMessage());
        Assert.assertEquals(null, e.getCause());
    }

    @Test
    public void testExceptionWithMessageAndCause() {
        Exception cause = new Exception();
        RpcException e = new RpcException("test", cause);
        Assert.assertEquals("test", e.getMessage());
        Assert.assertEquals(cause, e.getCause());
    }

    @Test
    public void testExceptionWithMessageAndArgs() {
        RpcException e = new RpcException("test %s", 168);
        Assert.assertEquals("test 168", e.getMessage());
        Assert.assertEquals(null, e.getCause());
    }

    @Test
    public void testExceptionWithMessageAndArgsAndCause() {
        Exception cause = new Exception();
        RpcException e = new RpcException("test %s", cause, 168);
        Assert.assertEquals("test 168", e.getMessage());
        Assert.assertEquals(cause, e.getCause());
    }
}
