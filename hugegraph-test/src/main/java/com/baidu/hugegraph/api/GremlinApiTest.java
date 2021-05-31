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

import java.util.List;
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

        String script = "schema=hugegraph.schema();"
                + "schema.propertyKey('name').asText().ifNotExist().create();"
                + "schema.propertyKey('age').asInt().ifNotExist().create();"
                + "schema.propertyKey('city').asUUID().ifNotExist().create();"
                + "schema.propertyKey('lang').asText().ifNotExist().create();"
                + "schema.propertyKey('date').asText().ifNotExist().create();"
                + "schema.propertyKey('price').asInt().ifNotExist().create();"
                + "person=schema.vertexLabel('person').properties('name','age','city').useCustomizeUuidId().ifNotExist().create();"
                + "knows=schema.edgeLabel('knows').sourceLabel('person').targetLabel('person').properties('date').ifNotExist().create();"
                + "marko=hugegraph.addVertex(T.id,'835e1153928149578691cf79258e90eb',T.label,'person','name','marko','age',29,'city','135e1153928149578691cf79258e90eb');"
                + "vadas=hugegraph.addVertex(T.id,'935e1153928149578691cf79258e90eb',T.label,'person','name','vadas','age',27,'city','235e1153928149578691cf79258e90eb');"
                + "marko.addEdge('knows',vadas,'date','20160110');";
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
                + "\"gremlin\":\""
                + "def auth = hugegraph.hugegraph().authManager();"
                + "def admin = auth.findUser('admin');"
                + "hugegraph.clearBackend();"
                + "hugegraph.initBackend();"
                + "auth.createUser(admin);\","
                + "\"bindings\":{},"
                + "\"language\":\"gremlin-groovy\","
                + "\"aliases\":{\"g\":\"__g_hugegraph\"}}";
        assertResponseStatus(200, client().post(path, body));

        body = "{"
                + "\"gremlin\":\"hugegraph.serverStarted("
                + "IdGenerator.of('server1'), NodeRole.MASTER)\","
                + "\"bindings\":{},"
                + "\"language\":\"gremlin-groovy\","
                + "\"aliases\":{\"g\":\"__g_hugegraph\"}}";
        assertResponseStatus(200, client().post(path, body));
    }

    @Test
    public void testTruncate() {
        String body = "{"
                + "\"gremlin\":\"try {hugegraph.truncateBackend()} "
                + "catch (UnsupportedOperationException e) {}\","
                + "\"bindings\":{},"
                + "\"language\":\"gremlin-groovy\","
                + "\"aliases\":{\"g\":\"__g_hugegraph\"}}";
        assertResponseStatus(200, client().post(path, body));
    }

    @Test
    public void testSetVertexProperty() {
        String pkPath = "/graphs/hugegraph/schema/propertykeys/";
        // Cardinality single
        String foo = "{"
                + "\"name\": \"foo\","
                + "\"data_type\": \"TEXT\","
                + "\"cardinality\": \"SINGLE\","
                + "\"properties\":[]"
                + "}";
        assertResponseStatus(201, client().post(pkPath, foo));
        // Cardinality list
        String bar = "{"
                + "\"name\": \"bar\","
                + "\"data_type\": \"TEXT\","
                + "\"cardinality\": \"LIST\","
                + "\"properties\":[]"
                + "}";
        assertResponseStatus(201, client().post(pkPath, bar));

        String vlPath = "/graphs/hugegraph/schema/vertexlabels/";
        String vertexLabel = "{"
                + "\"name\": \"person\","
                + "\"id_strategy\": \"CUSTOMIZE_STRING\","
                + "\"properties\": [\"foo\", \"bar\"]"
                + "}";
        assertResponseStatus(201, client().post(vlPath, vertexLabel));

        // Not supply cardinality
        String body = "{"
                + "\"gremlin\":\"g.addV('person').property(T.id, '1')"
                + ".property('foo', '123').property('bar', '123')\","
                + "\"bindings\":{},"
                + "\"language\":\"gremlin-groovy\","
                + "\"aliases\":{\"g\":\"__g_hugegraph\"}}";
        assertResponseStatus(200, client().post(path, body));

        // Supply matched cardinality
        body = "{\"gremlin\":\"g.addV('person').property(T.id, '1')"
                + ".property(single, 'foo', '123')"
                + ".property(list, 'bar', '123')\","
                + "\"bindings\":{},"
                + "\"language\":\"gremlin-groovy\","
                + "\"aliases\":{\"g\":\"__g_hugegraph\"}}";
        assertResponseStatus(200, client().post(path, body));

        // Supply unmatch cardinality
        body = "{\"gremlin\":\"g.addV('person').property(T.id, '1')"
                + ".property(list, 'foo', '123')"
                + ".property(list, 'bar', '123')\","
                + "\"bindings\":{},"
                + "\"language\":\"gremlin-groovy\","
                + "\"aliases\":{\"g\":\"__g_hugegraph\"}}";
        assertResponseStatus(400, client().post(path, body));

        // NOTE: supply unmatch cardinality, but we give up the check
        body = "{\"gremlin\":\"g.addV('person').property(T.id, '1')"
                + ".property(single, 'foo', '123')"
                + ".property(single, 'bar', '123')\","
                + "\"bindings\":{},"
                + "\"language\":\"gremlin-groovy\","
                + "\"aliases\":{\"g\":\"__g_hugegraph\"}}";
        assertResponseStatus(200, client().post(path, body));
    }

    @Test
    public void testFileSerialize() {
        String body = "{"
                + "\"gremlin\":\"File file = new File('test.text')\","
                + "\"bindings\":{},"
                + "\"language\":\"gremlin-groovy\","
                + "\"aliases\":{\"g\":\"__g_hugegraph\"}}";
        Response r = client().post(path, body);
        String content = r.readEntity(String.class);
        Assert.assertTrue(content, r.getStatus() == 200);
        Map<?, ?> result = assertJsonContains(content, "result");
        @SuppressWarnings({ "unchecked", "rawtypes" })
        Map data = ((List<Map>) assertMapContains(result, "data")).get(0);
        Assert.assertEquals("test.text", data.get("file"));
    }
}
