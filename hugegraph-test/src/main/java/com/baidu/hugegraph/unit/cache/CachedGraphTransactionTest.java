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

package com.baidu.hugegraph.unit.cache;

import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertexProperty;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.baidu.hugegraph.HugeFactory;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.backend.cache.CachedGraphTransaction;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.testutil.Whitebox;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.IdStrategy;
import com.baidu.hugegraph.unit.BaseUnitTest;
import com.baidu.hugegraph.unit.FakeObjects;
import com.baidu.hugegraph.util.Events;

public class CachedGraphTransactionTest extends BaseUnitTest {

    private CachedGraphTransaction cache;
    private HugeGraphParams params;

    @Before
    public void setup() {
        HugeGraph graph = HugeFactory.open(FakeObjects.newConfig());
        this.params = Whitebox.getInternalState(graph, "params");
        this.cache = new CachedGraphTransaction(this.params,
                                                this.params.loadGraphStore());
    }

    @After
    public void teardown() throws Exception {
        this.cache().graph().clearBackend();
        this.cache().graph().close();
    }

    private CachedGraphTransaction cache() {
        Assert.assertNotNull(this.cache);
        return this.cache;
    }

    private HugeVertex newVertex(Id id) {
        HugeGraph graph = this.cache().graph();
        graph.schema().propertyKey("name").asText()
                .checkExist(false).create();
        graph.schema().vertexLabel("person")
                .idStrategy(IdStrategy.CUSTOMIZE_NUMBER)
                .properties("name").nullableKeys("name")
                .checkExist(false)
                .create();
        VertexLabel vl = graph.vertexLabel("person");
        return new HugeVertex(graph, id, vl);
    }

    private HugeEdge newEdge(HugeVertex out, HugeVertex in){
        HugeGraph graph = this.cache().graph();
        graph.schema().edgeLabel("person_know_person")
                .sourceLabel("person")
                .targetLabel("person")
                .checkExist(false)
                .create();
        return out.addEdge("person_know_person",in);
    }

    @Test
    public void testEventClear() throws Exception {
        CachedGraphTransaction cache = this.cache();

        cache.addVertex(this.newVertex(IdGenerator.of(1)));
        cache.addVertex(this.newVertex(IdGenerator.of(2)));
        cache.commit();

        Assert.assertTrue(cache.queryVertices(IdGenerator.of(1)).hasNext());
        Assert.assertTrue(cache.queryVertices(IdGenerator.of(2)).hasNext());
        Assert.assertEquals(2L,
                            Whitebox.invoke(cache, "verticesCache", "size"));

        this.params.graphEventHub().notify(Events.CACHE, "clear", null).get();

        Assert.assertEquals(0L,
                            Whitebox.invoke(cache, "verticesCache", "size"));

        Assert.assertTrue(cache.queryVertices(IdGenerator.of(1)).hasNext());
        Assert.assertEquals(1L,
                            Whitebox.invoke(cache, "verticesCache", "size"));
        Assert.assertTrue(cache.queryVertices(IdGenerator.of(2)).hasNext());
        Assert.assertEquals(2L,
                            Whitebox.invoke(cache, "verticesCache", "size"));
    }

    @Test
    public void testEventInvalid() throws Exception {
        CachedGraphTransaction cache = this.cache();

        cache.addVertex(this.newVertex(IdGenerator.of(1)));
        cache.addVertex(this.newVertex(IdGenerator.of(2)));
        cache.commit();

        Assert.assertTrue(cache.queryVertices(IdGenerator.of(1)).hasNext());
        Assert.assertTrue(cache.queryVertices(IdGenerator.of(2)).hasNext());
        Assert.assertEquals(2L,
                            Whitebox.invoke(cache, "verticesCache", "size"));

        this.params.graphEventHub().notify(Events.CACHE, "invalid",
                                           HugeType.VERTEX, IdGenerator.of(1))
                   .get();

        Assert.assertEquals(1L,
                            Whitebox.invoke(cache, "verticesCache", "size"));
        Assert.assertTrue(cache.queryVertices(IdGenerator.of(2)).hasNext());
        Assert.assertEquals(1L,
                            Whitebox.invoke(cache, "verticesCache", "size"));
        Assert.assertTrue(cache.queryVertices(IdGenerator.of(1)).hasNext());
        Assert.assertEquals(2L,
                            Whitebox.invoke(cache, "verticesCache", "size"));
    }


    @Test
    public void testClearEdgeCacheWhenDeleteVertex(){
        CachedGraphTransaction cache = this.cache();
        HugeVertex v1 = this.newVertex(IdGenerator.of(1));
        HugeVertex v2 = this.newVertex(IdGenerator.of(2));
        HugeVertex v3 = this.newVertex(IdGenerator.of(3));

        cache.addVertex(v1);
        cache.addVertex(v2);
        cache.commit();
        HugeEdge edge = this.newEdge(v1,v2);
        cache.addEdge(edge);
        cache.commit();
        Assert.assertTrue(cache.queryEdgesByVertex(IdGenerator.of(1)).hasNext());
        Assert.assertTrue(cache.queryEdgesByVertex(IdGenerator.of(2)).hasNext());

        Assert.assertEquals(2L,
                Whitebox.invoke(cache, "edgesCache", "size"));
        cache.removeVertex(v3);
        cache.commit();
        Assert.assertEquals(0L,
                Whitebox.invoke(cache, "edgesCache", "size"));

        Assert.assertTrue(cache.queryEdgesByVertex(IdGenerator.of(1)).hasNext());
        Assert.assertTrue(cache.queryEdgesByVertex(IdGenerator.of(2)).hasNext());
        Assert.assertEquals(2L,
                Whitebox.invoke(cache, "edgesCache", "size"));

        cache.removeVertex(v1);
        cache.commit();

        Assert.assertEquals(0L,
                Whitebox.invoke(cache, "edgesCache", "size"));
        Assert.assertFalse(cache.queryEdgesByVertex(IdGenerator.of(2)).hasNext());

    }

    @Test
    public void testClearEdgeCacheWhenUpdateVertex(){
        CachedGraphTransaction cache = this.cache();
        HugeVertex v1 = this.newVertex(IdGenerator.of(1));
        HugeVertex v2 = this.newVertex(IdGenerator.of(2));
        HugeVertex v3 = this.newVertex(IdGenerator.of(3));

        cache.addVertex(v1);
        cache.addVertex(v2);
        cache.commit();
        HugeEdge edge = this.newEdge(v1,v2);
        cache.addEdge(edge);
        cache.commit();
        Assert.assertTrue(cache.queryEdgesByVertex(IdGenerator.of(1)).hasNext());
        Assert.assertTrue(cache.queryEdgesByVertex(IdGenerator.of(2)).hasNext());

        Assert.assertEquals(2L,
                Whitebox.invoke(cache, "edgesCache", "size"));

        ///update property of v3
        cache.addVertexProperty(new HugeVertexProperty<>(v3,
                cache.graph().schema().getPropertyKey("name"),"test-name"));
        cache.commit();
        Assert.assertEquals(0L,
                Whitebox.invoke(cache, "edgesCache", "size"));

        Assert.assertTrue(cache.queryEdgesByVertex(IdGenerator.of(1)).hasNext());
        Assert.assertTrue(cache.queryEdgesByVertex(IdGenerator.of(2)).hasNext());
        Assert.assertEquals(2L,
                Whitebox.invoke(cache, "edgesCache", "size"));

        cache.addVertexProperty(new HugeVertexProperty<>(v1,
                cache.graph().schema().getPropertyKey("name"),"test-name"));
        cache.commit();

        Assert.assertEquals(0L,
                Whitebox.invoke(cache, "edgesCache", "size"));
        String name = cache.queryEdgesByVertex(IdGenerator.of(1)).next().outVertex()
                .value("name");
        Assert.assertEquals("test-name",name);


    }
}

