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
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.baidu.hugegraph.core;

import java.util.List;
import java.util.function.Consumer;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.core.FakeObjects.FakeEdge;
import com.baidu.hugegraph.core.FakeObjects.FakeVertex;

public class Utils {

    public static boolean containsId(List<Vertex> vertexes, Id id) {
        for (Vertex v : vertexes) {
            if (v.id().equals(id)) {
                return true;
            }
        }
        return false;
    }

    public static boolean contains(List<Vertex> vertexes,
            FakeVertex fakeVertex) {
        for (Vertex v : vertexes) {
            if (fakeVertex.equalsVertex(v)) {
                return true;
            }
        }
        return false;
    }

    public static boolean contains(List<Edge> edges, FakeEdge fakeEdge) {
        for (Edge e : edges) {
            if (fakeEdge.equalsEdge(e)) {
                return true;
            }
        }
        return false;
    }

    @FunctionalInterface
    interface ThrowableRunnable {
        void run() throws Throwable;
    }

    public static void assertThrows(
            Class<? extends Throwable> throwable,
            ThrowableRunnable runnable) {
        assertThrows(throwable, runnable, t -> {
            t.printStackTrace();
        });
    }

    public static void assertThrows(
            Class<? extends Throwable> throwable,
            ThrowableRunnable runnable,
            Consumer<Throwable> exceptionConsumer) {
        boolean fail = false;
        try {
            runnable.run();
            fail = true;
        } catch (Throwable t) {
            exceptionConsumer.accept(t);
            Assert.assertTrue(String.format(
                    "Bad exception type %s(expect %s)",
                    t.getClass(), throwable),
                    throwable.isInstance(t));
        }
        if (fail) {
            Assert.fail(String.format("No exception was thrown(expect %s)",
                    throwable));
        }
    }
}
