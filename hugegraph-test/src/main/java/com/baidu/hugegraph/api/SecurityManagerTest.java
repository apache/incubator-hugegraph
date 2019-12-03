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

package com.baidu.hugegraph.api;

import org.junit.Test;

import com.baidu.hugegraph.testutil.Assert;

public class SecurityManagerTest extends BaseApiTest {

    private static String path = "/gremlin";

    @Test
    public void testFile() {
        String bodyTemplate = "{"
                + "\"gremlin\":\"%s\","
                + "\"bindings\":{},"
                + "\"language\":\"gremlin-groovy\","
                + "\"aliases\":{\"g\":\"__g_hugegraph\"}}";

        String readFile = "new FileInputStream(new File(\\\"\\\"))";
        String body = String.format(bodyTemplate, readFile);
        Assert.assertEquals(403, client().post(path, body).getStatus());

        String readFd = "new FileInputStream(FileDescriptor.in)";
        body = String.format(bodyTemplate, readFd);
        Assert.assertEquals(403, client().post(path, body).getStatus());

        String writeFile = "new FileOutputStream(new File(\\\"\\\"))";
        body = String.format(bodyTemplate, writeFile);
        Assert.assertEquals(403, client().post(path, body).getStatus());

        String writeFd = "new FileOutputStream(FileDescriptor.out)";
        body = String.format(bodyTemplate, writeFd);
        Assert.assertEquals(403, client().post(path, body).getStatus());

        String deleteFile = "new File(\\\"\\\").delete()";
        body = String.format(bodyTemplate, deleteFile);
        Assert.assertEquals(403, client().post(path, body).getStatus());
    }
}
