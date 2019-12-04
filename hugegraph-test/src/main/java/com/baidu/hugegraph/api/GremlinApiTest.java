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

import java.util.Map;

import javax.ws.rs.core.Response;

import org.junit.Test;

import com.baidu.hugegraph.testutil.Assert;
import com.google.common.collect.ImmutableMap;

public class GremlinApiTest extends BaseApiTest {

    private static String path = "/gremlin";

    @Test
    public void testPost() {
        String body = "{"
                + "\"gremlin\":\"g.V()\","
                + "\"bindings\":{},"
                + "\"language\":\"gremlin-groovy\","
                + "\"aliases\":{\"g\":\"__g_hugegraph\"}}";
        assertResponseStatus(200, client().post(path, body));
    }

    @Test
    public void testGet() {
        Map<String, Object> params = ImmutableMap.of("gremlin",
                                     "hugegraph.traversal().V()");
        Response r = client().get(path, params);
        Assert.assertEquals(r.readEntity(String.class), 200, r.getStatus());
    }

    @Test
    public void testScript() {
        String bodyTemplate = "{"
                + "\"gremlin\":\"%s\","
                + "\"bindings\":{},"
                + "\"language\":\"gremlin-groovy\","
                + "\"aliases\":{\"g\":\"__g_hugegraph\"}}";

        String script = "schema=hugegraph.schema();" +
                "schema.propertyKey('name').asText().ifNotExist().create();" +
                "schema.propertyKey('age').asInt().ifNotExist().create();" +
                "schema.propertyKey('city').asUUID().ifNotExist().create();" +
                "schema.propertyKey('lang').asText().ifNotExist().create();" +
                "schema.propertyKey('date').asText().ifNotExist().create();" +
                "schema.propertyKey('price').asInt().ifNotExist().create();" +
                "person=schema.vertexLabel('person').properties('name','age'," +
                "'city').useCustomizeUuidId().ifNotExist().create();" +
                "knows=schema.edgeLabel('knows').sourceLabel('person').targetLabel('person').properties('date').ifNotExist().create();" +
                "marko=hugegraph.addVertex(T.id, '835e1153928149578691cf79258e90eb', T.label,'person','name','marko','age',29,'city','135e1153928149578691cf79258e90eb');" +
                "vadas=hugegraph.addVertex(T.id, '935e1153928149578691cf79258e90eb', T.label,'person','name','vadas','age',27,'city','235e1153928149578691cf79258e90eb');" +
                "marko.addEdge('knows',vadas,'date','20160110');";
        String body = String.format(bodyTemplate, script);
        assertResponseStatus(200, client().post(path, body));

        String queryV = "g.V()";
        body = String.format(bodyTemplate, queryV);
        assertResponseStatus(200, client().post(path, body));

        String queryE = "g.E()";
        body = String.format(bodyTemplate, queryE);
        assertResponseStatus(200, client().post(path, body));
    }

    @Test
    public void testClearAndInit() {
        String body = "{"
                + "\"gremlin\":\"hugegraph.clearBackend()\","
                + "\"bindings\":{},"
                + "\"language\":\"gremlin-groovy\","
                + "\"aliases\":{\"g\":\"__g_hugegraph\"}}";
        assertResponseStatus(200, client().post(path, body));

        body = "{"
                + "\"gremlin\":\"hugegraph.initBackend()\","
                + "\"bindings\":{},"
                + "\"language\":\"gremlin-groovy\","
                + "\"aliases\":{\"g\":\"__g_hugegraph\"}}";
        assertResponseStatus(200, client().post(path, body));
    }

    @Test
    public void testTruncate() {
        String body = "{"
                + "\"gremlin\":\"hugegraph.truncateBackend()\","
                + "\"bindings\":{},"
                + "\"language\":\"gremlin-groovy\","
                + "\"aliases\":{\"g\":\"__g_hugegraph\"}}";
        assertResponseStatus(200, client().post(path, body));
    }
}
