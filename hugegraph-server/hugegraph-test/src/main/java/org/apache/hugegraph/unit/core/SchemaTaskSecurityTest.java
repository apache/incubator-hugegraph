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
import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.store.BackendStore;
import org.apache.hugegraph.backend.tx.SchemaTransactionV2;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.id.Id;
import org.apache.hugegraph.id.IdGenerator;
import org.apache.hugegraph.security.HugeSecurityManager;
import org.apache.hugegraph.struct.schema.IndexLabel;
import org.apache.hugegraph.struct.schema.PropertyKey;
import org.apache.hugegraph.task.TaskScheduler;
import org.apache.hugegraph.type.define.DataType;
import org.apache.hugegraph.type.define.WriteType;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

public class SchemaTaskSecurityTest {

    @Test
    public void testV2SchemaTasksCanCreateWorkerFromGremlin() throws Exception {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        TaskScheduler scheduler = Mockito.mock(TaskScheduler.class);
        HugeGraphParams params = Mockito.mock(HugeGraphParams.class);
        BackendStore store = Mockito.mock(BackendStore.class);
        Mockito.when(params.loadGraphStore()).thenReturn(store);
        SchemaTransactionV2 transaction = Mockito.mock(SchemaTransactionV2.class,
                invocation -> "graphParams".equals(invocation.getMethod().getName()) ?
                              params : Mockito.CALLS_REAL_METHODS.answer(invocation));
        Mockito.doReturn(graph).when(transaction).graph();
        Mockito.when(graph.taskScheduler()).thenReturn(scheduler);
        Id taskId = IdGenerator.of(1L);
        Mockito.when(graph.getNextId(Mockito.any())).thenReturn(taskId);
        Mockito.when(graph.option(CoreOptions.TASK_SYNC_DELETION)).thenReturn(false);
        AtomicInteger workers = new AtomicInteger();
        Answer<Object> createWorker = invocation -> {
            // Exercise the cold executor's thread-group and thread checks,
            // without starting a worker or running a background schema job.
            Thread worker = Executors.defaultThreadFactory().newThread(() -> { });
            Assert.assertEquals(Thread.State.NEW, worker.getState());
            workers.incrementAndGet();
            return null;
        };
        Mockito.doAnswer(createWorker).when(scheduler).schedule(Mockito.any());
        Mockito.doAnswer(createWorker).when(store).createOlapTable(Mockito.any());
        Mockito.doAnswer(createWorker).when(store).clearOlapTable(Mockito.any());
        Mockito.doAnswer(createWorker).when(store).removeOlapTable(Mockito.any());
        SimpleBindings bindings = new SimpleBindings();
        bindings.put("tx", transaction);
        bindings.put("index", new IndexLabel(graph, IdGenerator.of(2L), "by_age"));
        PropertyKey propertyKey = new PropertyKey(graph, IdGenerator.of(3L), "score");
        propertyKey.dataType(DataType.INT);
        propertyKey.writeType(WriteType.OLAP_RANGE);
        bindings.put("pk", propertyKey);
        String[] scripts = {"tx.rebuildIndex(index)", "tx.createOlapPk(pk)",
                            "tx.clearOlapPk(pk)", "tx.removeOlapPk(pk)"};
        String[] directThreadScripts = {"new Thread()", "tx.createOlapPk(pk.id())",
                                        "tx.clearOlapPk(pk.id())", "tx.removeOlapPk(pk.id())"};
        GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
        // Load Groovy/mock metadata before restricting filesystem access.
        // Each schedule call still has to create a fresh worker Thread.
        for (String script : scripts) {
            engine.eval(script, bindings);
        }
        for (String script : directThreadScripts) {
            engine.eval(script, bindings);
        }
        engine.eval("System.getProperties()");
        workers.set(0);
        SecurityManager previous = System.getSecurityManager();
        String name = Thread.currentThread().getName();
        Thread.currentThread().setName("gremlin-server-exec-schema-security-test");
        System.setSecurityManager(new HugeSecurityManager());
        try {
            for (String script : scripts) {
                Assert.assertEquals(taskId, engine.eval(script, bindings));
            }
            Assert.assertEquals(scripts.length, workers.get());
            for (String script : directThreadScripts) {
                ScriptException denied = Assert.assertThrows(ScriptException.class,
                        () -> engine.eval(script, bindings));
                Assert.assertTrue(denied.getMessage().contains(
                        "Not allowed to access thread group via Gremlin"));
            }
            ScriptException denied = Assert.assertThrows(ScriptException.class,
                    () -> engine.eval("System.getProperties()"));
            Assert.assertTrue(denied.getMessage().contains(
                    "Not allowed to access system properties via Gremlin"));
        } finally {
            System.setSecurityManager(previous);
            Thread.currentThread().setName(name);
        }
    }
}
