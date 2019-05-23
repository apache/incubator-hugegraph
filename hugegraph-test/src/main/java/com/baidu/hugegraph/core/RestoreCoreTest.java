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

package com.baidu.hugegraph.core;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Test;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.exception.ExistedException;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.type.define.GraphMode;
import com.baidu.hugegraph.type.define.IdStrategy;

public class RestoreCoreTest extends BaseCoreTest {

    @After
    @Override
    public void teardown() throws Exception {
        super.teardown();
        graph().mode(GraphMode.NONE);
    }

    @Test
    public void testCreateVertexLabelWithId0InNoneMode() {
        HugeGraph graph = graph();
        graph.mode(GraphMode.NONE);
        Assert.assertThrows(IllegalArgumentException.class, () ->
            graph.schema().vertexLabel("person").id(0L).create()
        );
    }

    @Test
    public void testCreateVertexLabelWithId0InMergingMode() {
        HugeGraph graph = graph();
        graph.mode(GraphMode.MERGING);
        Assert.assertThrows(IllegalArgumentException.class, () ->
            graph.schema().vertexLabel("person").id(0L).create()
        );
    }

    @Test
    public void testCreateVertexLabelWithId0InRestoringMode() {
        HugeGraph graph = graph();
        graph.mode(GraphMode.RESTORING);
        Assert.assertThrows(IllegalArgumentException.class, () ->
            graph.schema().vertexLabel("person").id(0L).create()
        );
    }

    @Test
    public void testCreateVertexLabelWithNegativeIdInNoneMode() {
        HugeGraph graph = graph();
        graph.mode(GraphMode.NONE);
        Assert.assertThrows(IllegalStateException.class, () ->
            graph.schema().vertexLabel("person").id(-100L).create()
        );
    }

    @Test
    public void testCreateVertexLabelWithNegativeIdInMergingMode() {
        HugeGraph graph = graph();
        graph.mode(GraphMode.MERGING);
        Assert.assertThrows(IllegalStateException.class, () ->
            graph.schema().vertexLabel("person").id(-100L).create()
        );
    }

    @Test
    public void testCreateVertexLabelWithNegativeIdInRestoringMode() {
        HugeGraph graph = graph();
        graph.mode(GraphMode.RESTORING);
        Assert.assertThrows(IllegalStateException.class, () ->
            graph.schema().vertexLabel("person").id(-100L).create()
        );
    }

    @Test
    public void testCreateVertexLabelWithPositiveIdInNoneMode() {
        HugeGraph graph = graph();
        graph.mode(GraphMode.NONE);
        Assert.assertThrows(IllegalStateException.class, () ->
            graph.schema().vertexLabel("person").id(100L).create()
        );
    }

    @Test
    public void testCreateVertexLabelWithPositiveIdInMergingMode() {
        HugeGraph graph = graph();
        graph.mode(GraphMode.MERGING);
        Assert.assertThrows(IllegalStateException.class, () ->
            graph.schema().vertexLabel("person").id(100L).create()
        );
    }

    @Test
    public void testCreateVertexLabelWithPositiveIdInRestoringMode() {
        HugeGraph graph = graph();
        graph.mode(GraphMode.RESTORING);
        graph.schema().vertexLabel("person").id(100L).create();
    }

    @Test
    public void testCreateVertexLabelWithNonExistNameAndIdInRestoringMode() {
        HugeGraph graph = graph();
        graph.mode(GraphMode.RESTORING);
        graph.schema().vertexLabel("non-exist").id(100000L).create();
    }

    @Test
    public void testCreateVertexLabelWithNameExistWithIdInRestoringMode() {
        HugeGraph graph = graph();
        graph.mode(GraphMode.RESTORING);
        graph.schema().vertexLabel("person").id(100L).create();
        Assert.assertThrows(ExistedException.class, () ->
            graph.schema().vertexLabel("person").id(100L).create()
        );
    }

    @Test
    public void testCreateVertexLabelWithIdExistInRestoringMode() {
        HugeGraph graph = graph();
        graph.mode(GraphMode.RESTORING);
        graph.schema().vertexLabel("person").id(100L).create();
        Assert.assertThrows(ExistedException.class, () ->
            graph.schema().vertexLabel("person1").id(100L).create()
        );
    }

    @Test
    public void testCreateSystemVertexLabelWith0Id() {
        HugeGraph graph = graph();
        graph.mode(GraphMode.RESTORING);
        Assert.assertThrows(IllegalArgumentException.class, () ->
            graph.schema().vertexLabel("~person").id(0L).create()
        );
    }

    @Test
    public void testCreateSystemVertexLabelWithPositiveId() {
        HugeGraph graph = graph();
        graph.mode(GraphMode.RESTORING);
        Assert.assertThrows(IllegalStateException.class, () ->
            graph.schema().vertexLabel("~person").id(100L).build()
        );
    }

    @Test
    public void testCreateSystemVertexLabelWithNegativeId() {
        HugeGraph graph = graph();
        graph.mode(GraphMode.RESTORING);
        graph.schema().vertexLabel("~person").id(-100L).build();
    }

    @Test
    public void testCreateVertexWithAutomaticStrategyAndIdInNoneMode() {
        HugeGraph graph = graph();
        graph.schema().vertexLabel("person")
             .idStrategy(IdStrategy.AUTOMATIC).create();
        graph.mode(GraphMode.NONE);
        Assert.assertThrows(IllegalArgumentException.class, () ->
            graph.addVertex(T.label, "person", T.id, 100L)
        );
    }

    @Test
    public void testCreateVertexWithAutomaticStrategyAndIdInMergingMode() {
        HugeGraph graph = graph();
        graph.schema().vertexLabel("person")
             .idStrategy(IdStrategy.AUTOMATIC).create();
        graph.mode(GraphMode.MERGING);
        graph.addVertex(T.label, "person", T.id, 100L);
    }

    @Test
    public void testCreateVertexWithAutomaticStrategyAndIdInRestoringMode() {
        HugeGraph graph = graph();
        graph.schema().vertexLabel("person")
             .idStrategy(IdStrategy.AUTOMATIC).create();
        graph.mode(GraphMode.RESTORING);
        graph.addVertex(T.label, "person", T.id, 100L);
    }

    @Test
    public void testCreateVertexWithPrimaryKeyStrategyAndIdInNoneMode() {
        HugeGraph graph = graph();
        graph.schema().propertyKey("name").create();
        graph.schema().vertexLabel("person").properties("name")
             .idStrategy(IdStrategy.PRIMARY_KEY).primaryKeys("name").create();
        graph.mode(GraphMode.NONE);
        Assert.assertThrows(IllegalArgumentException.class, () ->
            graph.addVertex(T.label, "person", T.id, 100L, "name", "Tom")
        );
    }

    @Test
    public void testCreateVertexWithPrimaryKeyStrategyAndIdInMergingMode() {
        HugeGraph graph = graph();
        graph.schema().propertyKey("name").create();
        graph.schema().vertexLabel("person").properties("name")
             .idStrategy(IdStrategy.PRIMARY_KEY).primaryKeys("name").create();
        graph.mode(GraphMode.MERGING);
        Assert.assertThrows(IllegalArgumentException.class, () ->
            graph.addVertex(T.label, "person", T.id, 100L, "name", "Tom")
        );
    }

    @Test
    public void testCreateVertexWithPrimaryKeyStrategyAndIdInRestoringMode() {
        HugeGraph graph = graph();
        graph.schema().propertyKey("name").create();
        graph.schema().vertexLabel("person").properties("name")
             .idStrategy(IdStrategy.PRIMARY_KEY).primaryKeys("name").create();
        graph.mode(GraphMode.RESTORING);
        Assert.assertThrows(IllegalArgumentException.class, () ->
            graph.addVertex(T.label, "person", T.id, 100L, "name", "Tom")
        );
    }

    @Test
    public void testCreateVertexWithCustomizeStrategyAndIdInNoneMode() {
        HugeGraph graph = graph();
        graph.schema().vertexLabel("person")
             .idStrategy(IdStrategy.CUSTOMIZE_NUMBER).create();
        graph.mode(GraphMode.NONE);
        graph.addVertex(T.label, "person", T.id, 100L);
    }

    @Test
    public void testCreateVertexWithCustomizeStrategyAndIdInMergingMode() {
        HugeGraph graph = graph();
        graph.schema().vertexLabel("person")
             .idStrategy(IdStrategy.CUSTOMIZE_NUMBER).create();
        graph.mode(GraphMode.MERGING);
        graph.addVertex(T.label, "person", T.id, 100L);
    }

    @Test
    public void testCreateVertexWithCustomizeStrategyAndIdInRestoringMode() {
        HugeGraph graph = graph();
        graph.schema().vertexLabel("person")
             .idStrategy(IdStrategy.CUSTOMIZE_NUMBER).create();
        graph.mode(GraphMode.RESTORING);
        graph.addVertex(T.label, "person", T.id, 100L);
    }

    @Test
    public void testCreateEdgeLabelWithId0InNoneMode() {
        HugeGraph graph = graph();
        graph.schema().vertexLabel("person").create();
        graph.mode(GraphMode.NONE);
        Assert.assertThrows(IllegalArgumentException.class, () ->
                graph.schema().edgeLabel("knows").sourceLabel("person")
                     .targetLabel("person").id(0L)
                     .create()
        );
    }

    @Test
    public void testCreateEdgeLabelWithId0InMergingMode() {
        HugeGraph graph = graph();
        graph.schema().vertexLabel("person").create();
        graph.mode(GraphMode.MERGING);
        Assert.assertThrows(IllegalArgumentException.class, () ->
                graph.schema().edgeLabel("knows").sourceLabel("person")
                     .targetLabel("person").id(0L)
                     .create()
        );
    }

    @Test
    public void testCreateEdgeLabelWithId0InRestoringMode() {
        HugeGraph graph = graph();
        graph.schema().vertexLabel("person").create();
        graph.mode(GraphMode.RESTORING);
        Assert.assertThrows(IllegalArgumentException.class, () ->
                graph.schema().edgeLabel("knows").sourceLabel("person")
                     .targetLabel("person").id(0L)
                     .create()
        );
    }

    @Test
    public void testCreateEdgeLabelWithNegativeIdInNoneMode() {
        HugeGraph graph = graph();
        graph.schema().vertexLabel("person").create();
        graph.mode(GraphMode.NONE);
        Assert.assertThrows(IllegalStateException.class, () ->
                graph.schema().edgeLabel("knows").sourceLabel("person")
                     .targetLabel("person").id(-100L)
                     .create()
        );
    }

    @Test
    public void testCreateEdgeLabelWithNegativeIdInMergingMode() {
        HugeGraph graph = graph();
        graph.schema().vertexLabel("person").create();
        graph.mode(GraphMode.MERGING);
        Assert.assertThrows(IllegalStateException.class, () ->
                graph.schema().edgeLabel("knows").sourceLabel("person")
                     .targetLabel("person").id(-100L)
                     .create()
        );
    }

    @Test
    public void testCreateEdgeLabelWithNegativeIdInRestoringMode() {
        HugeGraph graph = graph();
        graph.schema().vertexLabel("person").create();
        graph.mode(GraphMode.RESTORING);
        Assert.assertThrows(IllegalStateException.class, () ->
                graph.schema().edgeLabel("knows").sourceLabel("person")
                     .targetLabel("person").id(-100L)
                     .create()
        );
    }

    @Test
    public void testCreateEdgeLabelWithPositiveIdInNoneMode() {
        HugeGraph graph = graph();
        graph.schema().vertexLabel("person").create();
        graph.mode(GraphMode.NONE);
        Assert.assertThrows(IllegalStateException.class, () ->
                graph.schema().edgeLabel("knows").sourceLabel("person")
                     .targetLabel("person").id(100L)
                     .create()
        );
    }

    @Test
    public void testCreateEdgeLabelWithPositiveIdInMergingMode() {
        HugeGraph graph = graph();
        graph.schema().vertexLabel("person").create();
        graph.mode(GraphMode.MERGING);
        Assert.assertThrows(IllegalStateException.class, () ->
                graph.schema().edgeLabel("knows").sourceLabel("person")
                     .targetLabel("person").id(100L)
                     .create()
        );
    }

    @Test
    public void testCreateEdgeLabelWithPositiveIdInRestoringMode() {
        HugeGraph graph = graph();
        graph.schema().vertexLabel("person").create();
        graph.mode(GraphMode.RESTORING);
        graph.schema().edgeLabel("knows").sourceLabel("person")
             .targetLabel("person").id(100L)
             .create();
    }

    @Test
    public void testCreateEdgeLabelWithNonExistNameAndIdInRestoringMode() {
        HugeGraph graph = graph();
        graph.schema().vertexLabel("person").create();
        graph.mode(GraphMode.RESTORING);
        graph.schema().edgeLabel("non-exist").sourceLabel("person")
             .targetLabel("person").id(100000L)
             .create();
    }

    @Test
    public void testCreateEdgeLabelWithNameExistWithIdInRestoringMode() {
        HugeGraph graph = graph();
        graph.schema().vertexLabel("person").create();
        graph.mode(GraphMode.RESTORING);
        graph.schema().edgeLabel("knows").sourceLabel("person")
             .targetLabel("person").id(100L)
             .create();
        Assert.assertThrows(ExistedException.class, () ->
                graph.schema().edgeLabel("knows").sourceLabel("person")
                     .targetLabel("person").id(100L)
                     .create()
        );
    }

    @Test
    public void testCreateEdgeLabelWithIdExistInRestoringMode() {
        HugeGraph graph = graph();
        graph.schema().vertexLabel("person").create();
        graph.mode(GraphMode.RESTORING);
        graph.schema().edgeLabel("knows").sourceLabel("person")
             .targetLabel("person").id(100L)
             .create();
        Assert.assertThrows(ExistedException.class, () ->
                graph.schema().edgeLabel("knows1").sourceLabel("person")
                     .targetLabel("person").id(100L)
                     .create()
        );
    }

    @Test
    public void testCreateEdgeWithIdInAllMode() {
        HugeGraph graph = graph();

        graph.schema().vertexLabel("person")
             .idStrategy(IdStrategy.AUTOMATIC).create();
        graph.schema().edgeLabel("knows").sourceLabel("person")
             .targetLabel("person").create();
        Vertex v1 = graph.addVertex(T.label, "person");
        Vertex v2 = graph.addVertex(T.label, "person");

        graph.mode(GraphMode.NONE);
        Assert.assertThrows(UnsupportedOperationException.class, () ->
            v1.addEdge("knows", v2, T.id, "id")
        );

        graph.mode(GraphMode.MERGING);
        Assert.assertThrows(UnsupportedOperationException.class, () ->
            v1.addEdge("knows", v2, T.id, "id")
        );

        graph.mode(GraphMode.RESTORING);
        Assert.assertThrows(UnsupportedOperationException.class, () ->
            v1.addEdge("knows", v2, T.id, "id")
        );
    }

    @Test
    public void testCreatePropertyKeyWithId0InNoneMode() {
        HugeGraph graph = graph();
        graph.mode(GraphMode.NONE);
        Assert.assertThrows(IllegalArgumentException.class, () ->
            graph.schema().propertyKey("name").id(0L).create()
        );
    }

    @Test
    public void testCreatePropertyKeyWithId0InMergingMode() {
        HugeGraph graph = graph();
        graph.mode(GraphMode.MERGING);
        Assert.assertThrows(IllegalArgumentException.class, () ->
            graph.schema().propertyKey("name").id(0L).create()
        );
    }

    @Test
    public void testCreatePropertyKeyWithId0InRestoringMode() {
        HugeGraph graph = graph();
        graph.mode(GraphMode.RESTORING);
        Assert.assertThrows(IllegalArgumentException.class, () ->
            graph.schema().propertyKey("name").id(0L).create()
        );
    }

    @Test
    public void testCreatePropertyKeyWithNegativeIdInNoneMode() {
        HugeGraph graph = graph();
        graph.mode(GraphMode.NONE);
        Assert.assertThrows(IllegalStateException.class, () ->
            graph.schema().propertyKey("name").id(-100L).create()
        );
    }

    @Test
    public void testCreatePropertyKeyWithNegativeIdInMergingMode() {
        HugeGraph graph = graph();
        graph.mode(GraphMode.MERGING);
        Assert.assertThrows(IllegalStateException.class, () ->
            graph.schema().propertyKey("name").id(-100L).create()
        );
    }

    @Test
    public void testCreatePropertyKeyWithNegativeIdInRestoringMode() {
        HugeGraph graph = graph();
        graph.mode(GraphMode.RESTORING);
        Assert.assertThrows(IllegalStateException.class, () ->
            graph.schema().propertyKey("name").id(-100L).create()
        );
    }

    @Test
    public void testCreatePropertyKeyWithPositiveIdInNoneMode() {
        HugeGraph graph = graph();
        graph.mode(GraphMode.NONE);
        Assert.assertThrows(IllegalStateException.class, () ->
            graph.schema().propertyKey("name").id(100L).create()
        );
    }

    @Test
    public void testCreatePropertyKeyWithPositiveIdInMergingMode() {
        HugeGraph graph = graph();
        graph.mode(GraphMode.MERGING);
        Assert.assertThrows(IllegalStateException.class, () ->
            graph.schema().propertyKey("name").id(100L).create()

        );
    }

    @Test
    public void testCreatePropertyKeyWithPositiveIdInRestoringMode() {
        HugeGraph graph = graph();
        graph.mode(GraphMode.RESTORING);
        graph.schema().propertyKey("name").id(100L).create();
    }

    @Test
    public void testCreatePropertyKeyWithNonExistNameAndIdInRestoringMode() {
        HugeGraph graph = graph();
        graph.mode(GraphMode.RESTORING);
        graph.schema().propertyKey("non-exist").id(100000L).create();

    }

    @Test
    public void testCreatePropertyKeyWithNameExistWithIdInRestoringMode() {
        HugeGraph graph = graph();
        graph.mode(GraphMode.RESTORING);
        graph.schema().propertyKey("name").id(100L).create();
        Assert.assertThrows(ExistedException.class, () ->
            graph.schema().propertyKey("name").id(1000L).create()
        );
    }

    @Test
    public void testCreatePropertyKeyWithIdExistInRestoringMode() {
        HugeGraph graph = graph();
        graph.mode(GraphMode.RESTORING);
        graph.schema().propertyKey("name").id(100L).create();
        Assert.assertThrows(ExistedException.class, () ->
            graph.schema().propertyKey("name1").id(100L).create()
        );
    }

    @Test
    public void testCreateIndexLabelWithId0InNoneMode() {
        HugeGraph graph = graph();
        graph.schema().propertyKey("name").create();
        graph.schema().propertyKey("city").create();
        graph.schema().vertexLabel("person").properties("name", "city")
             .primaryKeys("name").create();
        graph.mode(GraphMode.NONE);
        Assert.assertThrows(IllegalArgumentException.class, () ->
            graph.schema().indexLabel("personByCity")
                 .onV("person").by("city").secondary().id(0L)
                 .create()
        );
    }

    @Test
    public void testCreateIndexLabelWithId0InMergingMode() {
        HugeGraph graph = graph();
        graph.schema().propertyKey("name").create();
        graph.schema().propertyKey("city").create();
        graph.schema().vertexLabel("person").properties("name", "city")
             .primaryKeys("name").create();
        graph.mode(GraphMode.MERGING);
        Assert.assertThrows(IllegalArgumentException.class, () ->
            graph.schema().indexLabel("personByCity")
                 .onV("person").by("city").secondary().id(0L)
                 .create()
        );
    }

    @Test
    public void testCreateIndexLabelWithId0InRestoringMode() {
        HugeGraph graph = graph();
        graph.schema().propertyKey("name").create();
        graph.schema().propertyKey("city").create();
        graph.schema().vertexLabel("person").properties("name", "city")
             .primaryKeys("name").create();
        graph.mode(GraphMode.RESTORING);
        Assert.assertThrows(IllegalArgumentException.class, () ->
            graph.schema().indexLabel("personByCity")
                 .onV("person").by("city").secondary().id(0L)
                 .create()
        );
    }

    @Test
    public void testCreateIndexLabelWithNegativeIdInNoneMode() {
        HugeGraph graph = graph();
        graph.schema().propertyKey("name").create();
        graph.schema().propertyKey("city").create();
        graph.schema().vertexLabel("person").properties("name", "city")
             .primaryKeys("name").create();
        graph.mode(GraphMode.NONE);
        Assert.assertThrows(IllegalStateException.class, () ->
            graph.schema().indexLabel("personByCity")
                 .onV("person").by("city").secondary().id(-100L)
                 .create()
        );
    }

    @Test
    public void testCreateIndexLabelWithNegativeIdInMergingMode() {
        HugeGraph graph = graph();
        graph.schema().propertyKey("name").create();
        graph.schema().propertyKey("city").create();
        graph.schema().vertexLabel("person").properties("name", "city")
             .primaryKeys("name").create();
        graph.mode(GraphMode.MERGING);
        Assert.assertThrows(IllegalStateException.class, () ->
            graph.schema().indexLabel("personByCity")
                 .onV("person").by("city").secondary().id(-100L)
                 .create()
        );
    }

    @Test
    public void testCreateIndexLabelWithNegativeIdInRestoringMode() {
        HugeGraph graph = graph();
        graph.schema().propertyKey("name").create();
        graph.schema().propertyKey("city").create();
        graph.schema().vertexLabel("person").properties("name", "city")
             .primaryKeys("name").create();
        graph.mode(GraphMode.RESTORING);
        Assert.assertThrows(IllegalStateException.class, () ->
            graph.schema().indexLabel("personByCity")
                 .onV("person").by("city").secondary().id(-100L)
                 .create()
        );
    }

    @Test
    public void testCreateIndexLabelWithPositiveIdInNoneMode() {
        HugeGraph graph = graph();
        graph.schema().propertyKey("name").create();
        graph.schema().propertyKey("city").create();
        graph.schema().vertexLabel("person").properties("name", "city")
             .primaryKeys("name").create();
        graph.mode(GraphMode.NONE);
        Assert.assertThrows(IllegalStateException.class, () ->
            graph.schema().indexLabel("personByCity")
                 .onV("person").by("city").secondary().id(100L)
                 .create()
        );
    }

    @Test
    public void testCreateIndexLabelWithPositiveIdInMergingMode() {
        HugeGraph graph = graph();
        graph.schema().propertyKey("name").create();
        graph.schema().propertyKey("city").create();
        graph.schema().vertexLabel("person").properties("name", "city")
             .primaryKeys("name").create();
        graph.mode(GraphMode.MERGING);
        Assert.assertThrows(IllegalStateException.class, () ->
            graph.schema().indexLabel("personByCity")
                 .onV("person").by("city").secondary().id(100L)
                 .create()
        );
    }

    @Test
    public void testCreateIndexLabelWithPositiveIdInRestoringMode() {
        HugeGraph graph = graph();
        graph.schema().propertyKey("name").create();
        graph.schema().propertyKey("city").create();
        graph.schema().vertexLabel("person").properties("name", "city")
             .primaryKeys("name").create();
        graph.mode(GraphMode.RESTORING);
        graph.schema().indexLabel("personByCity")
             .onV("person").by("city").secondary().id(100L)
             .create();
    }

    @Test
    public void testCreateIndexLabelWithNonExistNameAndIdInRestoringMode() {
        HugeGraph graph = graph();
        graph.schema().propertyKey("name").create();
        graph.schema().propertyKey("city").create();
        graph.schema().vertexLabel("person").properties("name", "city")
             .primaryKeys("name").create();
        graph.mode(GraphMode.RESTORING);
        graph.schema().indexLabel("non-exist")
             .onV("person").by("city").secondary().id(10000L)
             .create();
    }

    @Test
    public void testCreateIndexLabelWithNameExistWithIdInRestoringMode() {
        HugeGraph graph = graph();
        graph.schema().propertyKey("name").create();
        graph.schema().propertyKey("city").create();
        graph.schema().vertexLabel("person").properties("name", "city")
             .primaryKeys("name").create();
        graph.mode(GraphMode.RESTORING);
        graph.schema().indexLabel("personByCity")
             .onV("person").by("city").secondary().id(100L)
             .create();
        Assert.assertThrows(ExistedException.class, () ->
            graph.schema().indexLabel("personByCity")
                 .onV("person").by("city").secondary().id(100L)
                 .create()
        );
    }

    @Test
    public void testCreateIndexLabelWithIdExistInRestoringMode() {
        HugeGraph graph = graph();
        graph.schema().propertyKey("name").create();
        graph.schema().propertyKey("city").create();
        graph.schema().vertexLabel("person").properties("name", "city")
             .primaryKeys("name").create();
        graph.mode(GraphMode.RESTORING);
        graph.schema().indexLabel("personByCity")
             .onV("person").by("city").secondary().id(100L)
             .create();
        Assert.assertThrows(ExistedException.class, () ->
            graph.schema().indexLabel("personByCity1")
                 .onV("person").by("city").secondary().id(100L)
                 .create()
        );
    }

}
