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

package com.baidu.hugegraph.unit.rest;

import java.util.Map;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.internal.util.collection.ImmutableMultivaluedMap;
import org.junit.Test;
import org.mockito.Mockito;

import com.baidu.hugegraph.rest.RestResult;
import com.baidu.hugegraph.rest.SerializeException;
import com.baidu.hugegraph.testutil.Assert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class RestResultTest {

    @Test
    public void testStatus() {
        RestResult result = newRestResult(200);
        Assert.assertEquals(200, result.status());
    }

    @Test
    public void testHeaders() {
        MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
        headers.add("key1", "value1-1");
        headers.add("key1", "value1-2");
        headers.add("key2", "value2");
        RestResult result = newRestResult(200, headers);
        Assert.assertEquals(200, result.status());
        Assert.assertEquals(headers, result.headers());
    }

    @Test
    public void testContent() {
        String content = "{\"name\": \"marko\"}";
        RestResult result = newRestResult(200, content);
        Assert.assertEquals(200, result.status());
        Assert.assertEquals(content, result.content());
        Assert.assertEquals(ImmutableMap.of("name", "marko"),
                            result.readObject(Map.class));
    }

    @Test
    public void testContentWithException() {
        String content = "{illegal key: \"marko\"}";
        RestResult result = newRestResult(200, content);
        Assert.assertEquals(200, result.status());
        Assert.assertEquals(content, result.content());
        Assert.assertThrows(SerializeException.class, () -> {
            result.readObject(Map.class);
        });
    }

    @Test
    public void testContentList() {
        String content = "{\"names\": [\"marko\", \"josh\", \"lop\"]}";
        RestResult result = newRestResult(200, content);
        Assert.assertEquals(200, result.status());
        Assert.assertEquals(content, result.content());
        Assert.assertEquals(ImmutableList.of("marko", "josh", "lop"),
                            result.readList("names", String.class));

        content = "[\"marko\", \"josh\", \"lop\"]";
        result = newRestResult(200, content);
        Assert.assertEquals(200, result.status());
        Assert.assertEquals(content, result.content());
        Assert.assertEquals(ImmutableList.of("marko", "josh", "lop"),
                            result.readList(String.class));
    }

    @Test
    public void testContentListWithException() {
        String content = "{\"names\": [\"marko\", \"josh\", \"lop\"]}";
        RestResult result = newRestResult(200, content);
        Assert.assertEquals(200, result.status());
        Assert.assertEquals(content, result.content());
        Assert.assertThrows(SerializeException.class, () -> {
            result.readList("unexitsed key", String.class);
        });

        content = "{\"names\": [marko, josh, \"lop\"]}";
        RestResult result2 = newRestResult(200, content);
        Assert.assertEquals(200, result2.status());
        Assert.assertEquals(content, result2.content());
        Assert.assertThrows(SerializeException.class, () -> {
            result2.readList("names", String.class);
        });

        content = "[marko, josh, \"lop\"]";
        RestResult result3 = newRestResult(200, content);
        Assert.assertEquals(200, result3.status());
        Assert.assertEquals(content, result3.content());
        Assert.assertThrows(SerializeException.class, () -> {
            result3.readList(String.class);
        });
    }

    private static RestResult newRestResult(int status) {
        return newRestResult(status, "", ImmutableMultivaluedMap.empty());
    }

    private static RestResult newRestResult(int status, String content) {
        return newRestResult(status, content, ImmutableMultivaluedMap.empty());
    }

    private static RestResult newRestResult(int status,
                                            MultivaluedMap<String, Object> h) {
        return newRestResult(status, "", h);
    }

    private static RestResult newRestResult(int status, String content,
                                            MultivaluedMap<String, Object> h) {
        Response response = Mockito.mock(Response.class);
        Mockito.when(response.getStatus()).thenReturn(status);
        Mockito.when(response.getHeaders()).thenReturn(h);
        Mockito.when(response.readEntity(String.class))
               .thenReturn(content);
        return new RestResult(response);
    }
}
