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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.event.EventHub;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.hugegraph.space.GraphSpace;
import org.apache.hugegraph.space.Service;
import org.apache.hugegraph.task.TaskScheduler;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.fasterxml.jackson.databind.ObjectMapper;

public class GraphManagerClearGraphSpaceTest {

    /**
     * Graph and service keys are "{graphspace}-{name}", and a graph space
     * name may be a prefix of another one, so clearing "gs" must leave the
     * graphs and services of "gs2" and "gs_x" alone
     */
    @Test
    public void testClearKeepsGraphsOfSpacesNamedLikeIt() {
        HugeGraph cleared = mockGraph("gs-g");
        HugeGraph kept = mockGraph("gs2-g");
        HugeGraph keptUnderscore = mockGraph("gs_x-g");
        MetaManager meta = Mockito.mock(MetaManager.class);

        // A mock hub: the executor of EventHub is static, and a test that ran
        // earlier in the suite may have shut it down (HugeFactory.shutdown)
        GraphManager manager = new GraphManager(
                new HugeConfig(new PropertiesConfiguration()),
                Mockito.mock(EventHub.class));
        try {
            Whitebox.setInternalState(manager, "PDExist", true);
            Whitebox.setInternalState(manager, "metaManager", meta);
            Whitebox.setInternalState(manager, "localGraphs",
                                      Collections.emptySet());
            Map<String, GraphSpace> spaces =
                    Whitebox.getInternalState(manager, "graphSpaces");
            for (String space : new String[]{"gs", "gs2", "gs_x"}) {
                spaces.put(space, new GraphSpace(space));
            }
            Map<String, Graph> graphs =
                    Whitebox.getInternalState(manager, "graphs");
            graphs.put("gs-g", cleared);
            graphs.put("gs2-g", kept);
            graphs.put("gs_x-g", keptUnderscore);
            Map<String, Service> services =
                    Whitebox.getInternalState(manager, "services");
            services.put("gs-s", Mockito.mock(Service.class));
            services.put("gs2-s", Mockito.mock(Service.class));
            services.put("gs_x-s", Mockito.mock(Service.class));

            manager.clearGraphSpace("gs");

            Mockito.verify(cleared).clearBackend();
            Mockito.verify(kept, Mockito.never()).clearBackend();
            Mockito.verify(keptUnderscore, Mockito.never()).clearBackend();
            Assert.assertFalse(graphs.containsKey("gs-g"));
            Assert.assertTrue(graphs.containsKey("gs2-g"));
            Assert.assertTrue(graphs.containsKey("gs_x-g"));

            Mockito.verify(meta).service("gs", "s");
            Mockito.verify(meta, Mockito.never()).service("gs2", "s");
            Mockito.verify(meta, Mockito.never()).service("gs_x", "s");
        } finally {
            manager.close();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testRemoveEventEvictsOnlyDeletedSpaceOnEveryReplica() {
        MetaManager meta = Mockito.mock(MetaManager.class);
        Object response = new Object();
        Mockito.when(meta.extractGraphSpacesFromResponse(response)).thenReturn(List.of("gs"));
        for (int replica = 0; replica < 2; replica++) {
            GraphManager manager = new GraphManager(
                    new HugeConfig(new PropertiesConfiguration()), Mockito.mock(EventHub.class));
            try {
                Whitebox.setInternalState(manager, "metaManager", meta);
                Map<String, GraphSpace> spaces = Whitebox.getInternalState(manager, "graphSpaces");
                spaces.put("gs", new GraphSpace("gs"));
                spaces.put("gs2", new GraphSpace("gs2"));
                Map<String, Service> services = Whitebox.getInternalState(manager, "services");
                services.put("gs-s", Mockito.mock(Service.class));
                services.put("gs2-s", Mockito.mock(Service.class));
                Whitebox.invoke(GraphManager.class, "listenMetaChanges", manager);
                ArgumentCaptor<Consumer> listener = ArgumentCaptor.forClass(Consumer.class);
                Mockito.verify(meta, Mockito.atLeastOnce()).listenGraphSpaceRemove(listener.capture());
                listener.getValue().accept(response);
                listener.getValue().accept(response);
                Assert.assertFalse(manager.graphSpaces().contains("gs"));
                Assert.assertTrue(manager.graphSpaces().contains("gs2"));
                Assert.assertFalse(services.containsKey("gs-s"));
                Assert.assertTrue(services.containsKey("gs2-s"));
            } finally {
                manager.close();
            }
        }
        Mockito.verify(meta, Mockito.never()).removeGraphSpaceConfig(Mockito.anyString());
        Mockito.verify(meta, Mockito.never()).notifyGraphSpaceRemove(Mockito.anyString());
    }

    @Test
    public void testDelayedRemoveKeepsRecreatedSpace() {
        MetaManager meta = Mockito.mock(MetaManager.class);
        Object response = new Object();
        Mockito.when(meta.extractGraphSpacesFromResponse(response)).thenReturn(List.of("gs"));
        GraphSpace recreated = new GraphSpace("gs");
        recreated.storageLimit = 200;
        Mockito.when(meta.graphSpace("gs")).thenReturn(recreated);
        GraphManager manager = new GraphManager(
                new HugeConfig(new PropertiesConfiguration()), Mockito.mock(EventHub.class));
        try {
            Whitebox.setInternalState(manager, "metaManager", meta);
            Whitebox.setInternalState(manager, "PDExist", true);
            Map<String, GraphSpace> spaces = Whitebox.getInternalState(manager, "graphSpaces");
            GraphSpace deleted = new GraphSpace("gs");
            deleted.storageLimit = 50;
            spaces.put("gs", deleted);
            Map<String, Service> services = Whitebox.getInternalState(manager, "services");
            services.put("gs-s", Mockito.mock(Service.class));
            Whitebox.invoke(GraphManager.class, "graphSpaceRemoveHandler", manager, response);
            Assert.assertSame(recreated, manager.graphSpace("gs"));
            Assert.assertEquals(200, manager.graphSpace("gs").storageLimit());
            Assert.assertFalse(services.containsKey("gs-s"));
        } finally {
            manager.close();
        }
    }

    @Test
    public void testDefaultSpaceUsesFiniteDefaultsWithoutMigratingExistingSpace() {
        GraphManager manager = Mockito.mock(GraphManager.class, Mockito.CALLS_REAL_METHODS);
        MetaManager meta = Mockito.mock(MetaManager.class);
        Whitebox.setInternalState(manager, "metaManager", meta);
        Mockito.when(meta.graphSpaceConfigs()).thenReturn(Collections.emptyMap());
        Mockito.doAnswer(invocation -> invocation.getArgument(0))
               .when(manager).createGraphSpace(Mockito.any(GraphSpace.class));
        Whitebox.invoke(GraphManager.class, "createDefaultGraphSpaceIfNeeded", manager,
                        new HugeConfig(new PropertiesConfiguration()));
        ArgumentCaptor<GraphSpace> created = ArgumentCaptor.forClass(GraphSpace.class);
        Mockito.verify(manager).createGraphSpace(created.capture());
        GraphSpace space = created.getValue();
        Assert.assertEquals(128, space.cpuLimit());
        Assert.assertEquals(128, space.memoryLimit());
        Assert.assertEquals(128, space.computeCpuLimit());
        Assert.assertEquals(128, space.computeMemoryLimit());
        Assert.assertEquals(10240, space.storageLimit());
        Assert.assertEquals(100, space.maxGraphNumber());
        Assert.assertEquals(100, space.maxRoleNumber());
        Mockito.when(meta.graphSpaceConfigs()).thenReturn(Map.of("DEFAULT", space));
        Whitebox.invoke(GraphManager.class, "createDefaultGraphSpaceIfNeeded", manager,
                        new HugeConfig(new PropertiesConfiguration()));
        Mockito.verify(manager, Mockito.times(1)).createGraphSpace(Mockito.any(GraphSpace.class));
    }

    @Test
    public void testApiComputeDefaultsInheritRequestedResources() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        Class<?> type = Class.forName("org.apache.hugegraph.api.space.GraphSpaceAPI$JsonGraphSpace");
        for (int cpu : new int[]{4, 128, 256}) {
            Object request = mapper.readValue(String.format(
                    "{\"name\":\"gs\",\"cpu_limit\":%d,\"memory_limit\":%d}", cpu, cpu * 2), type);
            Whitebox.invoke(type, "checkCreate", request, false);
            GraphSpace space = Whitebox.invoke(type, "toGraphSpace", request, "admin");
            Assert.assertEquals(cpu, space.computeCpuLimit());
            Assert.assertEquals(cpu * 2, space.computeMemoryLimit());
        }
        Object request = mapper.readValue("{\"name\":\"gs\"}", type);
        Whitebox.invoke(type, "checkCreate", request, false);
        GraphSpace defaults = Whitebox.invoke(type, "toGraphSpace", request, "admin");
        Assert.assertEquals(128, defaults.computeCpuLimit());
        Assert.assertEquals(128, defaults.computeMemoryLimit());
        Assert.assertEquals(10240, defaults.storageLimit());
        request = mapper.readValue("{\"name\":\"gs\",\"compute_cpu_limit\":16,\"compute_memory_limit\":32}", type);
        GraphSpace explicit = Whitebox.invoke(type, "toGraphSpace", request, "admin");
        Assert.assertEquals(16, explicit.computeCpuLimit());
        Assert.assertEquals(32, explicit.computeMemoryLimit());
        request = mapper.readValue("{\"name\":\"gs\",\"compute_cpu_limit\":0,\"compute_memory_limit\":0}", type);
        GraphSpace inherit = Whitebox.invoke(type, "toGraphSpace", request, "admin");
        Assert.assertEquals(0, inherit.computeCpuLimit());
        Assert.assertEquals(0, inherit.computeMemoryLimit());
    }

    private static HugeGraph mockGraph(String spaceGraphName) {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        // No tasks to drain: the drop closes task admission and goes on
        TaskScheduler scheduler = Mockito.mock(TaskScheduler.class);
        Mockito.when(scheduler.close()).thenReturn(true);
        Mockito.when(graph.taskScheduler()).thenReturn(scheduler);
        Mockito.when(graph.spaceGraphName()).thenReturn(spaceGraphName);
        return graph;
    }
}
