/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.unit.core;

import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import javax.script.ScriptException;
import javax.script.SimpleBindings;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.tx.SchemaTransactionV2;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.security.HugeSecurityManager;
import org.apache.hugegraph.struct.schema.IndexLabel;
import org.apache.hugegraph.task.TaskScheduler;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class SchemaTaskSecurityTest {

    @Test
    public void testV2IndexRebuildCanCreateWorkerFromGremlin() throws Exception {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        TaskScheduler scheduler = Mockito.mock(TaskScheduler.class);
        SchemaTransactionV2 transaction = Mockito.mock(
                SchemaTransactionV2.class, Mockito.CALLS_REAL_METHODS);
        Mockito.doReturn(graph).when(transaction).graph();
        Mockito.when(graph.taskScheduler()).thenReturn(scheduler);
        Id taskId = IdGenerator.of(1L);
        Mockito.when(graph.getNextId(Mockito.any())).thenReturn(taskId);
        Mockito.when(graph.option(CoreOptions.TASK_SYNC_DELETION)).thenReturn(false);
        AtomicInteger workers = new AtomicInteger();
        Mockito.doAnswer(invocation -> {
            // Exercise the cold executor's thread-group and thread checks,
            // without starting a worker or running a background schema job.
            Thread worker = Executors.defaultThreadFactory().newThread(() -> { });
            Assert.assertEquals(Thread.State.NEW, worker.getState());
            workers.incrementAndGet();
            return null;
        }).when(scheduler).schedule(Mockito.any());
        SimpleBindings bindings = new SimpleBindings();
        bindings.put("tx", transaction);
        bindings.put("index", new IndexLabel(graph, IdGenerator.of(2L), "by_age"));
        GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
        // Load Groovy/mock metadata before restricting filesystem access.
        // Each schedule call still has to create a fresh worker Thread.
        engine.eval("tx.rebuildIndex(index)", bindings);
        engine.eval("new Thread()");
        engine.eval("System.getProperties()");
        workers.set(0);
        SecurityManager previous = System.getSecurityManager();
        String name = Thread.currentThread().getName();
        Thread.currentThread().setName("gremlin-server-exec-schema-security-test");
        System.setSecurityManager(new HugeSecurityManager());
        try {
            Assert.assertEquals(taskId, engine.eval("tx.rebuildIndex(index)", bindings));
            Assert.assertEquals(1, workers.get());
            ScriptException denied = Assert.assertThrows(ScriptException.class,
                    () -> engine.eval("new Thread()"));
            Assert.assertTrue(denied.getMessage().contains(
                    "Not allowed to access thread group via Gremlin"));
            denied = Assert.assertThrows(ScriptException.class,
                    () -> engine.eval("System.getProperties()"));
            Assert.assertTrue(denied.getMessage().contains(
                    "Not allowed to access system properties via Gremlin"));
        } finally {
            System.setSecurityManager(previous);
            Thread.currentThread().setName(name);
        }
    }
}
