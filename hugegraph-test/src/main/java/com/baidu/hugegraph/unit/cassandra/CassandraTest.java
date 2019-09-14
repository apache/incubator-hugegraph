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

package com.baidu.hugegraph.unit.cassandra;

import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.baidu.hugegraph.backend.store.cassandra.CassandraOptions;
import com.baidu.hugegraph.backend.store.cassandra.CassandraStore;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.testutil.Whitebox;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class CassandraTest {

    @Before
    public void setup() {
        // pass
    }

    @After
    public void teardown() {
        // pass
    }

    @Test
    public void testParseRepilcaWithSimpleStrategy() {
        String strategy = CassandraOptions.CASSANDRA_STRATEGY.name();
        String replica = CassandraOptions.CASSANDRA_REPLICATION.name();

        Configuration conf = Mockito.mock(PropertiesConfiguration.class);
        Mockito.when(conf.getKeys())
               .thenReturn(ImmutableList.of(strategy, replica).iterator());
        Mockito.when(conf.getProperty(strategy))
               .thenReturn("SimpleStrategy");
        Mockito.when(conf.getProperty(replica))
               .thenReturn(ImmutableList.of("5"));
        HugeConfig config = new HugeConfig(conf);

        Map<String, Object> result = Whitebox.invokeStatic(CassandraStore.class,
                                                           "parseReplica",
                                                           config);

        Map<String, Object> expected = ImmutableMap.of(
                                       "class", "SimpleStrategy",
                                       "replication_factor", 5);
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testParseRepilcaWithNetworkTopologyStrategy() {
        String strategy = CassandraOptions.CASSANDRA_STRATEGY.name();
        String replica = CassandraOptions.CASSANDRA_REPLICATION.name();

        Configuration conf = Mockito.mock(PropertiesConfiguration.class);
        Mockito.when(conf.getKeys())
               .thenReturn(ImmutableList.of(strategy, replica).iterator());
        Mockito.when(conf.getProperty(strategy))
               .thenReturn("NetworkTopologyStrategy");
        Mockito.when(conf.getProperty(replica))
               .thenReturn(ImmutableList.of("dc1:2", "dc2:1"));
        HugeConfig config = new HugeConfig(conf);

        Map<String, Object> result = Whitebox.invokeStatic(CassandraStore.class,
                                                           "parseReplica",
                                                           config);

        Map<String, Object> expected = ImmutableMap.of(
                                       "class", "NetworkTopologyStrategy",
                                       "dc1", 2,
                                       "dc2", 1);
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testParseRepilcaWithSimpleStrategyAndEmptyReplica() {
        String strategy = CassandraOptions.CASSANDRA_STRATEGY.name();
        String replica = CassandraOptions.CASSANDRA_REPLICATION.name();

        Configuration conf = Mockito.mock(PropertiesConfiguration.class);
        Mockito.when(conf.getKeys())
               .thenReturn(ImmutableList.of(strategy, replica).iterator());
        Mockito.when(conf.getProperty(strategy))
               .thenReturn("SimpleStrategy");
        Mockito.when(conf.getProperty(replica))
               .thenReturn(ImmutableList.of(""));
        HugeConfig config = new HugeConfig(conf);

        Assert.assertThrows(RuntimeException.class, () -> {
            Whitebox.invokeStatic(CassandraStore.class, "parseReplica", config);
        });
    }

    @Test
    public void testParseRepilcaWithSimpleStrategyAndDoubleReplica() {
        String strategy = CassandraOptions.CASSANDRA_STRATEGY.name();
        String replica = CassandraOptions.CASSANDRA_REPLICATION.name();

        Configuration conf = Mockito.mock(PropertiesConfiguration.class);
        Mockito.when(conf.getKeys())
               .thenReturn(ImmutableList.of(strategy, replica).iterator());
        Mockito.when(conf.getProperty(strategy))
               .thenReturn("SimpleStrategy");
        Mockito.when(conf.getProperty(replica))
               .thenReturn(ImmutableList.of("1.5"));
        HugeConfig config = new HugeConfig(conf);

        Assert.assertThrows(RuntimeException.class, () -> {
            Whitebox.invokeStatic(CassandraStore.class, "parseReplica", config);
        });
    }

    @Test
    public void testParseRepilcaWithSimpleStrategyAndStringReplica() {
        String strategy = CassandraOptions.CASSANDRA_STRATEGY.name();
        String replica = CassandraOptions.CASSANDRA_REPLICATION.name();

        Configuration conf = Mockito.mock(PropertiesConfiguration.class);
        Mockito.when(conf.getKeys())
               .thenReturn(ImmutableList.of(strategy, replica).iterator());
        Mockito.when(conf.getProperty(strategy))
               .thenReturn("SimpleStrategy");
        Mockito.when(conf.getProperty(replica))
               .thenReturn(ImmutableList.of("string"));
        HugeConfig config = new HugeConfig(conf);

        Assert.assertThrows(RuntimeException.class, () -> {
            Whitebox.invokeStatic(CassandraStore.class, "parseReplica", config);
        });
    }

    @Test
    public void testParseRepilcaWithNetworkTopologyStrategyAndStringReplica() {
        String strategy = CassandraOptions.CASSANDRA_STRATEGY.name();
        String replica = CassandraOptions.CASSANDRA_REPLICATION.name();

        Configuration conf = Mockito.mock(PropertiesConfiguration.class);
        Mockito.when(conf.getKeys())
               .thenReturn(ImmutableList.of(strategy, replica).iterator());
        Mockito.when(conf.getProperty(strategy))
               .thenReturn("NetworkTopologyStrategy");
        Mockito.when(conf.getProperty(replica))
               .thenReturn(ImmutableList.of("dc1:2", "dc2:string"));
        HugeConfig config = new HugeConfig(conf);

        Assert.assertThrows(RuntimeException.class, () -> {
            Whitebox.invokeStatic(CassandraStore.class, "parseReplica", config);
        });
    }

    @Test
    public void testParseRepilcaWithNetworkTopologyStrategyWithoutDatacenter() {
        String strategy = CassandraOptions.CASSANDRA_STRATEGY.name();
        String replica = CassandraOptions.CASSANDRA_REPLICATION.name();

        Configuration conf = Mockito.mock(PropertiesConfiguration.class);
        Mockito.when(conf.getKeys())
               .thenReturn(ImmutableList.of(strategy, replica).iterator());
        Mockito.when(conf.getProperty(strategy))
               .thenReturn("NetworkTopologyStrategy");
        Mockito.when(conf.getProperty(replica))
               .thenReturn(ImmutableList.of(":2", "dc2:1"));
        HugeConfig config = new HugeConfig(conf);

        Assert.assertThrows(RuntimeException.class, () -> {
            Whitebox.invokeStatic(CassandraStore.class, "parseReplica", config);
        });
    }

    @Test
    public void testParseRepilcaWithNetworkTopologyStrategyAndEmptyReplica() {
        String strategy = CassandraOptions.CASSANDRA_STRATEGY.name();
        String replica = CassandraOptions.CASSANDRA_REPLICATION.name();

        Configuration conf = Mockito.mock(PropertiesConfiguration.class);
        Mockito.when(conf.getKeys())
               .thenReturn(ImmutableList.of(strategy, replica).iterator());
        Mockito.when(conf.getProperty(strategy))
               .thenReturn("NetworkTopologyStrategy");
        Mockito.when(conf.getProperty(replica))
               .thenReturn(ImmutableList.of("dc1:", "dc2:1"));
        HugeConfig config = new HugeConfig(conf);

        Assert.assertThrows(RuntimeException.class, () -> {
            Whitebox.invokeStatic(CassandraStore.class, "parseReplica", config);
        });
    }

    @Test
    public void testParseRepilcaWithNetworkTopologyStrategyAndDoubleReplica() {
        String strategy = CassandraOptions.CASSANDRA_STRATEGY.name();
        String replica = CassandraOptions.CASSANDRA_REPLICATION.name();

        Configuration conf = Mockito.mock(PropertiesConfiguration.class);
        Mockito.when(conf.getKeys())
               .thenReturn(ImmutableList.of(strategy, replica).iterator());
        Mockito.when(conf.getProperty(strategy))
               .thenReturn("NetworkTopologyStrategy");
        Mockito.when(conf.getProperty(replica))
               .thenReturn(ImmutableList.of("dc1:3.5", "dc2:1"));
        HugeConfig config = new HugeConfig(conf);

        Assert.assertThrows(RuntimeException.class, () -> {
            Whitebox.invokeStatic(CassandraStore.class, "parseReplica", config);
        });
    }
}
