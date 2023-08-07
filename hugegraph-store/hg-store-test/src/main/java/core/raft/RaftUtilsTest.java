/*
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

package core.raft;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Collections;
import java.util.List;

import org.apache.hugegraph.store.raft.util.RaftUtils;
import org.junit.Before;
import org.junit.Test;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.NodeManager;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.NodeImpl;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.util.Endpoint;

import util.UnitTestBase;

public class RaftUtilsTest {
    final String ip = "127.0.0.1";
    final int port = 12456;
    final String dataPath = "tmp/raftUtils";

    @Before
    public void setUp() {

        UnitTestBase.deleteDir(new File(dataPath));
        new File(dataPath).mkdirs();
    }

    @Test
    public void testGetAllEndpoints1() throws InterruptedException {
        final Endpoint addr = new Endpoint(ip, port);
        final PeerId peer = new PeerId(addr, 0);
        NodeManager.getInstance().addAddress(addr);
        final NodeOptions nodeOptions = new NodeOptions();
        nodeOptions.setFsm(new MockStateMachine());
        nodeOptions.setLogUri(this.dataPath + File.separator + "log");
        nodeOptions.setRaftMetaUri(this.dataPath + File.separator + "meta");
        nodeOptions.setSnapshotUri(this.dataPath + File.separator + "snapshot");
        nodeOptions.setInitialConf(new Configuration(Collections.singletonList(peer)));
        final Node node = new NodeImpl("unittest", new PeerId(addr, 0));
        assertTrue(node.init(nodeOptions));
        node.isLeader(true);

        // Run the test
        final List<String> result = RaftUtils.getAllEndpoints(node);
        // Verify the results
        assertEquals(1, result.size());
        node.shutdown();
        node.join();

    }

    @Test
    public void testGetAllEndpoints2() {
        final Endpoint addr = new Endpoint(ip, port);
        final PeerId peer = new PeerId(addr, 0);

        Configuration conf = new Configuration(Collections.singletonList(peer));
        // Run the test
        final List<String> result = RaftUtils.getAllEndpoints(conf);

        // Verify the results
        assertEquals(1, result.size());
    }

    @Test
    public void testGetPeerEndpoints1() throws InterruptedException {
        final Endpoint addr = new Endpoint(ip, port);
        final PeerId peer = new PeerId(addr, 0);
        NodeManager.getInstance().addAddress(addr);
        final NodeOptions nodeOptions = new NodeOptions();
        nodeOptions.setFsm(new MockStateMachine());
        nodeOptions.setLogUri(this.dataPath + File.separator + "log");
        nodeOptions.setRaftMetaUri(this.dataPath + File.separator + "meta");
        nodeOptions.setSnapshotUri(this.dataPath + File.separator + "snapshot");
        nodeOptions.setInitialConf(new Configuration(Collections.singletonList(peer)));
        final Node node = new NodeImpl("unittest", new PeerId(addr, 0));
        assertTrue(node.init(nodeOptions));
        node.isLeader(true);


        // Run the test
        final List<String> result = RaftUtils.getPeerEndpoints(node);
        // Verify the results
        assertEquals(1, result.size());
        node.shutdown();
        node.join();
    }

    @Test
    public void testGetPeerEndpoints2() {
        // Setup
        final Configuration conf = new Configuration(List.of(new PeerId("ip", 0, 0, 0)),
                                                     List.of(new PeerId("ip", 0, 0, 0)));

        // Run the test
        final List<String> result = RaftUtils.getPeerEndpoints(conf);

        // Verify the results
        assertEquals(1, result.size());
    }

    @Test
    public void testGetLearnerEndpoints1() throws InterruptedException {
        // Setup
        final Endpoint addr = new Endpoint(ip, port);
        final PeerId peer = new PeerId(addr, 0);
        final PeerId peer2 = new PeerId(new Endpoint(ip, 13456), 0);
        NodeManager.getInstance().addAddress(addr);
        final NodeOptions nodeOptions = new NodeOptions();
        nodeOptions.setFsm(new MockStateMachine());
        nodeOptions.setLogUri(this.dataPath + File.separator + "log");
        nodeOptions.setRaftMetaUri(this.dataPath + File.separator + "meta");
        nodeOptions.setSnapshotUri(this.dataPath + File.separator + "snapshot");
        nodeOptions.setInitialConf(new Configuration(Collections.singletonList(peer),
                                                     Collections.singletonList(peer2)));
        final Node node = new NodeImpl("unittest", new PeerId(addr, 0));
        assertTrue(node.init(nodeOptions));
        node.isLeader(true);
        // Run the test
        final List<String> result = RaftUtils.getLearnerEndpoints(node);

        // Verify the results
        assertEquals(1, result.size());
        node.shutdown();
        node.join();
    }

    @Test
    public void testGetLearnerEndpoints2() {
        // Setup
        final Configuration conf = new Configuration(List.of(new PeerId("ip", 0, 0, 0)),
                                                     List.of(new PeerId("ip", 0, 0, 0)));

        // Run the test
        final List<String> result = RaftUtils.getLearnerEndpoints(conf);

        // Verify the results
        assertEquals(1, result.size());
    }

    @Test
    public void testConfigurationEquals() {
        // Setup
        final Configuration oldConf = new Configuration(List.of(new PeerId("ip", 0, 0, 0)),
                                                        List.of(new PeerId("ip", 0, 0, 0)));
        final Configuration newConf = new Configuration(List.of(new PeerId("ip", 0, 0, 0)),
                                                        List.of(new PeerId("ip", 0, 0, 0)));

        // Run the test
        final boolean result = RaftUtils.configurationEquals(oldConf, newConf);

        // Verify the results
        assertTrue(result);
    }
}
