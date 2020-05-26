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

import org.junit.Test;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.auth.HugePermission;
import com.baidu.hugegraph.auth.HugeResource;
import com.baidu.hugegraph.auth.HugeResource.NameObject;
import com.baidu.hugegraph.auth.HugeTarget;
import com.baidu.hugegraph.auth.HugeUser;
import com.baidu.hugegraph.auth.ResourceObject;
import com.baidu.hugegraph.auth.ResourceObject.ResourceType;
import com.baidu.hugegraph.auth.RolePermission;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.testutil.Whitebox;
import com.baidu.hugegraph.type.define.IdStrategy;
import com.baidu.hugegraph.unit.FakeObjects;
import com.google.common.collect.ImmutableMap;

public class RolePermissionTest {

    @Test
    public void testBuiltinAdmin() {
        RolePermission admin = RolePermission.admin();
        RolePermission role1 = RolePermission.role("admin", HugePermission.ANY);
        Assert.assertEquals(admin, role1);
        Assert.assertSame(admin, RolePermission.builtin(admin));
        Assert.assertSame(admin, RolePermission.builtin(role1));

        RolePermission role = RolePermission.fromJson("{\"roles\":{\"admin\":{\"ANY\":[{\"type\":\"ALL\",\"label\":\"write\",\"properties\":null}]}}");
        Assert.assertTrue(roleContains(admin, role));

        RolePermission role2 = RolePermission.all("admin");
        Assert.assertSame(admin, RolePermission.builtin(role2));
        Assert.assertTrue(roleContains(admin, role2));
        Assert.assertTrue(roleContains(role2, role));

        RolePermission hg = RolePermission.all("hg1");
        RolePermission role3 = RolePermission.fromJson("{\"roles\":{\"hg1\":{\"ANY\":[{\"type\":\"ALL\",\"label\":\"write\",\"properties\":null}]}}");
        Assert.assertSame(hg, RolePermission.builtin(hg));
        Assert.assertSame(hg, RolePermission.fromJson(hg));
        Assert.assertTrue(roleContains(hg, role3));

        /*
         * NOTE: admin role not match graph role
         * if want do this, rely on upper-layer special judgment
         */
        Assert.assertFalse(roleContains(admin, hg));
    }

    @Test
    public void testBuiltinNone() {
        RolePermission none = RolePermission.none();
        RolePermission role1 = RolePermission.role("none", HugePermission.NONE);
        Assert.assertEquals(none, role1);
        Assert.assertSame(none, RolePermission.builtin(none));
        Assert.assertSame(none, RolePermission.builtin(role1));

        Assert.assertEquals("{\"roles\":{\"none\":{\"NONE\":[{\"type\":\"ALL\",\"label\":\"*\",\"properties\":null}]}}}", none.toJson());
        RolePermission role = RolePermission.fromJson("{\"roles\":{\"none\":{\"NONE\":[{\"type\":\"ALL\",\"label\":\"write\",\"properties\":null}]}}");
        Assert.assertTrue(roleContains(none, role));
    }

    @Test
    public void testContains() {
        String json = "{\"roles\":{\"hugegraph\":{\"READ\":[{\"type\":\"EDGE\",\"label\":\"write\",\"properties\":null},{\"type\":\"PROPERTY_KEY\",\"label\":\"*\",\"properties\":null},{\"type\":\"VERTEX_LABEL\",\"label\":\"*\",\"properties\":null},{\"type\":\"EDGE_LABEL\",\"label\":\"*\",\"properties\":null},{\"type\":\"INDEX_LABEL\",\"label\":\"*\",\"properties\":null},{\"type\":\"VERTEX\",\"label\":\"person\",\"properties\":{\"city\":\"Beijing\",\"age\":\"P.gte(20)\"}},{\"type\":\"VERTEX_LABEL\",\"label\":\"*\",\"properties\":null},{\"type\":\"PROPERTY_KEY\",\"label\":\"*\",\"properties\":null}],\"WRITE\":[{\"type\":\"VERTEX\",\"label\":\"person\",\"properties\":{\"city\":\"Beijing\",\"age\":\"P.gte(20)\"}},{\"type\":\"VERTEX_LABEL\",\"label\":\"*\",\"properties\":null},{\"type\":\"PROPERTY_KEY\",\"label\":\"*\",\"properties\":null}],\"EXECUTE\":[{\"type\":\"GREMLIN\",\"label\":\"*\",\"properties\":null}]},\"hugegraph1\":{\"READ\":[]}}}";

        RolePermission role = RolePermission.fromJson(json);

        RolePermission r1 = RolePermission.fromJson(json);
        Assert.assertEquals(role, r1);
        Assert.assertTrue(roleContains(role, r1));

        RolePermission r2 = RolePermission.fromJson("{\"roles\":{\"hugegraph\":{\"READ\":[{\"type\":\"EDGE\",\"label\":\"write\",\"properties\":null}]}}");
        Assert.assertTrue(roleContains(role, r2));

        RolePermission r3 = RolePermission.fromJson("{\"roles\":{\"hugegraph\":{\"READ\":[{\"type\":\"EDGE\",\"label\":\"write\",\"properties\":{\"date\":\"2018-8-8\"}}]}}");
        Assert.assertTrue(roleContains(role, r3));

        RolePermission r4 = RolePermission.fromJson("{\"roles\":{\"hugegraph\":{\"WRITE\":[{\"type\":\"VERTEX\",\"label\":\"person\",\"properties\":{\"city\":\"Beijing\",\"age\":\"P.gte(20)\"}}]}}");
        Assert.assertTrue(roleContains(role, r4));

        RolePermission r5 = RolePermission.fromJson("{\"roles\":{\"hugegraph\":{\"WRITE\":[{\"type\":\"VERTEX\",\"label\":\"person\",\"properties\":{\"city\":\"Beijing\",\"age\":\"P.gte(21)\"}}]}}");
        Assert.assertFalse(roleContains(role, r5));

        RolePermission r6 = RolePermission.fromJson("{\"roles\":{\"hugegraph\":{\"WRITE\":[{\"type\":\"VERTEX\",\"label\":\"person\",\"properties\":null}]}}");
        Assert.assertFalse(roleContains(role, r6));

        RolePermission r7 = RolePermission.fromJson("{\"roles\":{\"hugegraph\":{\"WRITE\":[{\"type\":\"VERTEX\",\"label\":\"person2\",\"properties\":{\"city\":\"Beijing\",\"age\":\"P.gte(20)\"}}]}}");
        Assert.assertFalse(roleContains(role, r7));

        RolePermission r8 = RolePermission.fromJson("{\"roles\":{\"hugegraph\":{\"WRITE\":[{\"type\":\"EDGE\",\"label\":\"person\",\"properties\":{\"city\":\"Beijing\",\"age\":\"P.gte(20)\"}}]}}");
        Assert.assertFalse(roleContains(role, r8));

        role = RolePermission.fromJson("{\"roles\":{\"hugegraph\":{\"READ\":[{\"type\":\"ALL\",\"label\":\"write\",\"properties\":null}]}}");
        RolePermission r9 = RolePermission.fromJson("{\"roles\":{\"hugegraph\":{\"READ\":[{\"type\":\"ALL\",\"label\":\"write\",\"properties\":null}]}}");
        Assert.assertTrue(roleContains(role, r9));

        RolePermission r10 = RolePermission.fromJson("{\"roles\":{\"hugegraph\":{\"READ\":[{\"type\":\"EDGE\",\"label\":\"write\",\"properties\":null}]}}");
        Assert.assertTrue(roleContains(role, r10));

        RolePermission r11 = RolePermission.fromJson("{\"roles\":{\"hugegraph\":{\"READ\":[{\"type\":\"VERTEX\",\"label\":\"write\",\"properties\":null}]}}");
        Assert.assertTrue(roleContains(role, r11));

        RolePermission r12 = RolePermission.fromJson("{\"roles\":{\"hugegraph\":{\"READ\":[{\"type\":\"VERTEX\",\"label\":\"person\",\"properties\":null}]}}");
        Assert.assertFalse(roleContains(role, r12));

        RolePermission r13 = RolePermission.fromJson("{\"roles\":{\"hugegraph\":{\"WRITE\":[{\"type\":\"VERTEX\",\"label\":\"write\",\"properties\":null}]}}");
        Assert.assertFalse(roleContains(role, r13));

        RolePermission r14 = RolePermission.fromJson("{\"roles\":{\"hugegraph2\":{\"READ\":[{\"type\":\"VERTEX\",\"label\":\"write\",\"properties\":null}]}}");
        Assert.assertFalse(roleContains(role, r14));
    }

    @Test
    public void testHugeResource() {
        HugeResource r = new HugeResource(ResourceType.VERTEX, "person",
                                          ImmutableMap.of("city", "Beijing"));
        String json = "{\"type\":\"VERTEX\",\"label\":\"person\",\"properties\":{\"city\":\"Beijing\"}}";
        Assert.assertEquals(json, r.toString());
        Assert.assertEquals(r, HugeResource.parseResource(json));

        HugeResource r1 = new HugeResource(null, null, null);
        HugeResource r2 = new HugeResource(null, null, null);
        String nullJson = "{\"type\":null,\"label\":null,\"properties\":null}";
        Assert.assertEquals(nullJson, r1.toString());
        Assert.assertEquals(r1, r2);

        HugeResource r3 = HugeResource.parseResource(nullJson);
        Assert.assertEquals(r1, r3);

        Assert.assertThrows(HugeException.class, () -> {
            new HugeResource(ResourceType.VERTEX, "person",
                             ImmutableMap.of("city", "P.(1)"));
        }, e -> {
            Assert.assertContains("Invalid predicate: P.(1)",
                                  e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            String resource = "{\"type\":\"VERTEX\",\"label\":\"person\",\"properties\":{\"city\":\"P.(1)\"}}";
            HugeResource.parseResource(resource);
        }, e -> {
            Assert.assertContains("Invalid predicate: P.(1)",
                                  e.getMessage());
        });

        Assert.assertThrows(HugeException.class, () -> {
            String resources = "[{\"type\":\"VERTEX\",\"label\":\"person\",\"properties\":{\"city\":\"P.(1)\"}}]";
            HugeResource.parseResources(resources);
        }, e -> {
            Assert.assertContains("Invalid predicate: P.(1)",
                                  e.getMessage());
        });
    }

    @Test
    public void testHugeResourceFilter() {
        HugeResource all = HugeResource.ALL;

        // common
        ResourceObject<?> r1 = ResourceObject.of("g1", ResourceType.GREMLIN,
                                                 NameObject.ANY);
        Assert.assertTrue(all.filter(r1));

        ResourceObject<?> r2 = ResourceObject.of("g1", ResourceType.META,
                                                 NameObject.of("test"));
        Assert.assertTrue(all.filter(r2));

        HugeResource page = new HugeResource(ResourceType.META,
                                             "page", null);
        Assert.assertFalse(page.filter(r2));

        ResourceObject<?> r3 = ResourceObject.of("g1", ResourceType.META,
                                                 NameObject.of("page"));
        Assert.assertTrue(page.filter(r3));
    }

    @Test
    public void testHugeResourceFilterSchema() {
        HugeResource all = HugeResource.ALL;

        // schema
        HugeResource vlp = new HugeResource(ResourceType.VERTEX_LABEL,
                                            "p-.*", null);

        ResourceObject<?> r3 = ResourceObject.of("g1",
                                                 ResourceType.VERTEX_LABEL,
                                                 NameObject.of("test"));
        Assert.assertTrue(all.filter(r3));
        Assert.assertFalse(vlp.filter(r3));

        ResourceObject<?> r4 = ResourceObject.of("g1",
                                                 ResourceType.VERTEX_LABEL,
                                                 NameObject.of("p-test"));
        Assert.assertTrue(all.filter(r4));
        Assert.assertTrue(vlp.filter(r4));

        FakeObjects fo = new FakeObjects();

        VertexLabel vl1 = fo.newVertexLabel(IdGenerator.of("id1"), "person",
                                            IdStrategy.PRIMARY_KEY,
                                            IdGenerator.of("1"));
        ResourceObject<?> r5 = ResourceObject.of("g1", vl1);
        Assert.assertTrue(all.filter(r5));
        Assert.assertFalse(vlp.filter(r5));

        VertexLabel vl2 = fo.newVertexLabel(IdGenerator.of("id1"), "p-person",
                                            IdStrategy.PRIMARY_KEY,
                                            IdGenerator.of("1"));
        ResourceObject<?> r6 = ResourceObject.of("g1", vl2);
        Assert.assertTrue(all.filter(r6));
        Assert.assertTrue(vlp.filter(r6));
    }

    @Test
    public void testHugeResourceFilterVertexOrEdge() {
        HugeResource all = HugeResource.ALL;

        // vertex & edge
        FakeObjects fo = new FakeObjects();
        HugeEdge edge = fo.newEdge(1, 2);
        ResourceObject<?> r1 = ResourceObject.of("g1", edge.sourceVertex());
        ResourceObject<?> r2 = ResourceObject.of("g1", edge.targetVertex());
        ResourceObject<?> r3 = ResourceObject.of("g1", edge);

        Assert.assertTrue(all.filter(r1));
        Assert.assertTrue(all.filter(r2));
        Assert.assertTrue(all.filter(r3));

        HugeResource vr = new HugeResource(ResourceType.VERTEX,
                                           HugeResource.ANY, null);
        Assert.assertTrue(vr.filter(r1));
        Assert.assertTrue(vr.filter(r2));
        Assert.assertFalse(vr.filter(r3));

        vr = new HugeResource(ResourceType.VERTEX,
                              "person", null);
        Assert.assertTrue(vr.filter(r1));
        Assert.assertTrue(vr.filter(r2));
        Assert.assertFalse(vr.filter(r3));

        vr = new HugeResource(ResourceType.VERTEX,
                              "person", ImmutableMap.of("city", "Beijing"));
        Assert.assertTrue(vr.filter(r1));
        Assert.assertFalse(vr.filter(r2));
        Assert.assertFalse(vr.filter(r3));

        vr = new HugeResource(ResourceType.VERTEX,
                              "person", ImmutableMap.of("city", "Shanghai"));
        Assert.assertFalse(vr.filter(r1));
        Assert.assertTrue(vr.filter(r2));
        Assert.assertFalse(vr.filter(r3));

        vr = new HugeResource(ResourceType.VERTEX, "person", ImmutableMap.of(
                              "city", "P.within(\"Beijing\", \"Shanghai\")"));
        Assert.assertTrue(vr.filter(r1));
        Assert.assertTrue(vr.filter(r2));
        Assert.assertFalse(vr.filter(r3));

        vr = new HugeResource(ResourceType.VERTEX, "person",
                              ImmutableMap.of("age", "P.gt(18)"));
        Assert.assertFalse(vr.filter(r1));
        Assert.assertTrue(vr.filter(r2));
        Assert.assertFalse(vr.filter(r3));

        vr = new HugeResource(ResourceType.VERTEX, "person",
                              ImmutableMap.of("age", "P.between(20, 21)"));
        Assert.assertFalse(vr.filter(r1));
        Assert.assertTrue(vr.filter(r2));
        Assert.assertFalse(vr.filter(r3));

        vr = new HugeResource(ResourceType.VERTEX, "person",
                              ImmutableMap.of("age", "P.between(18, 21)"));
        Assert.assertTrue(vr.filter(r1));
        Assert.assertTrue(vr.filter(r2));
        Assert.assertFalse(vr.filter(r3));

        HugeResource er = new HugeResource(ResourceType.EDGE,
                                           "knows", null);
        Assert.assertFalse(er.filter(r1));
        Assert.assertFalse(er.filter(r2));
        Assert.assertTrue(er.filter(r3));

        er = new HugeResource(ResourceType.EDGE,
                              "knows", ImmutableMap.of("weight", "P.gt(0.7)"));
        Assert.assertFalse(er.filter(r1));
        Assert.assertFalse(er.filter(r2));
        Assert.assertTrue(er.filter(r3));

        er = new HugeResource(ResourceType.EDGE,
                              "knows", ImmutableMap.of("weight", "P.gt(0.8)"));
        Assert.assertFalse(er.filter(r1));
        Assert.assertFalse(er.filter(r2));
        Assert.assertFalse(er.filter(r3));

        er = new HugeResource(ResourceType.EDGE,
                              "knows", ImmutableMap.of("weight", "P.lt(0.8)"));
        Assert.assertFalse(er.filter(r1));
        Assert.assertFalse(er.filter(r2));
        Assert.assertTrue(er.filter(r3));
    }

    @Test
    public void testHugeResourceFilterUser() {
        HugeResource all = HugeResource.ALL;

        // user
        ResourceObject<?> r3 = ResourceObject.of("g1", ResourceType.USER_GROUP,
                                                 NameObject.ANY);
        Assert.assertFalse(all.filter(r3));

        HugeResource user = new HugeResource(ResourceType.USER_GROUP,
                                             HugeResource.ANY, null);
        Assert.assertTrue(user.filter(r3));

        ResourceObject<?> r4 = ResourceObject.of("g1", new HugeUser("fake"));
        Assert.assertTrue(user.filter(r4));

        HugeResource user2 = new HugeResource(ResourceType.USER_GROUP,
                                              "bj-.*", null);
        Assert.assertTrue(user2.filter(r3));
        Assert.assertFalse(user2.filter(r4));

        HugeResource user3 = new HugeResource(ResourceType.USER_GROUP,
                                              "fa.*", null);
        Assert.assertTrue(user3.filter(r3));
        Assert.assertTrue(user3.filter(r4));

        ResourceObject<?> r5 = ResourceObject.of("g1", new HugeTarget("g", ""));
        Assert.assertFalse(user.filter(r5));

        HugeResource root = new HugeResource(ResourceType.ROOT,
                                             HugeResource.ANY, null);
        Assert.assertTrue(root.filter(r3));
        Assert.assertTrue(root.filter(r4));
        Assert.assertTrue(root.filter(r5));
    }

    private boolean roleContains(RolePermission role, RolePermission other) {
        return Whitebox.invoke(RolePermission.class, "contains", role, other);
    }
}
