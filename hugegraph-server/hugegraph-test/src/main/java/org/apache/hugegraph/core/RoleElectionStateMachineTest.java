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

package org.apache.hugegraph.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.LockSupport;

import org.apache.hugegraph.masterelection.ClusterRole;
import org.apache.hugegraph.masterelection.ClusterRoleStore;
import org.apache.hugegraph.masterelection.Config;
import org.apache.hugegraph.masterelection.RoleElectionStateMachine;
import org.apache.hugegraph.masterelection.RoleListener;
import org.apache.hugegraph.masterelection.StandardRoleElectionStateMachine;
import org.apache.hugegraph.masterelection.StateMachineContext;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Utils;
import org.junit.Test;

public class RoleElectionStateMachineTest {

    private static class LogEntry {

        private final Integer epoch;

        private final String node;

        private final Role role;

        enum Role {
            master,
            worker,
            candidate,
            abdication,
            unknown
        }

        public LogEntry(Integer epoch, String node, Role role) {
            this.epoch = epoch;
            this.node = node;
            this.role = role;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof LogEntry)) {
                return false;
            }
            LogEntry logEntry = (LogEntry) obj;
            return Objects.equals(this.epoch, logEntry.epoch) &&
                   Objects.equals(this.node, logEntry.node) &&
                   this.role == logEntry.role;
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.epoch, this.node, this.role);
        }

        @Override
        public String toString() {
            return "LogEntry{" +
                   "epoch=" + this.epoch +
                   ", node='" + this.node + '\'' +
                   ", role=" + this.role +
                   '}';
        }
    }

    private static class TestConfig implements Config {

        private final String node;

        public TestConfig(String node) {
            this.node = node;
        }

        @Override
        public String node() {
            return this.node;
        }

        @Override
        public String url() {
            return "http://127.0.0.1:8080";
        }

        @Override
        public int exceedsFailCount() {
            return 2;
        }

        @Override
        public long randomTimeoutMillisecond() {
            return 400;
        }

        @Override
        public long heartBeatIntervalSecond() {
            return 1;
        }

        @Override
        public int masterDeadTimes() {
            return 5;
        }

        @Override
        public long baseTimeoutMillisecond() {
            return 100;
        }
    }

    @Test
    public void testStateMachine() throws InterruptedException {
        final int MAX_COUNT = 200;
        CountDownLatch stop = new CountDownLatch(4);
        List<LogEntry> logRecords = Collections.synchronizedList(new ArrayList<>(MAX_COUNT));
        List<String> masterNodes = Collections.synchronizedList(new ArrayList<>(MAX_COUNT));
        RoleListener callback = new RoleListener() {

            @Override
            public void onAsRoleMaster(StateMachineContext context) {
                Integer epochId = context.epoch();
                String node = context.node();
                logRecords.add(new LogEntry(epochId, node, LogEntry.Role.master));
                if (logRecords.size() > MAX_COUNT) {
                    context.stateMachine().shutdown();
                }
                Utils.println("master node: " + node);
                masterNodes.add(node);
            }

            @Override
            public void onAsRoleWorker(StateMachineContext context) {
                Integer epochId = context.epoch();
                String node = context.node();
                logRecords.add(new LogEntry(epochId, node, LogEntry.Role.worker));
                if (logRecords.size() > MAX_COUNT) {
                    context.stateMachine().shutdown();
                }
            }

            @Override
            public void onAsRoleCandidate(StateMachineContext context) {
                Integer epochId = context.epoch();
                String node = context.node();
                logRecords.add(new LogEntry(epochId, node, LogEntry.Role.candidate));
                if (logRecords.size() > MAX_COUNT) {
                    context.stateMachine().shutdown();
                }
            }

            @Override
            public void unknown(StateMachineContext context) {
                Integer epochId = context.epoch();
                String node = context.node();
                logRecords.add(new LogEntry(epochId, node, LogEntry.Role.unknown));
                if (logRecords.size() > MAX_COUNT) {
                    context.stateMachine().shutdown();
                }
            }

            @Override
            public void onAsRoleAbdication(StateMachineContext context) {
                Integer epochId = context.epoch();
                String node = context.node();
                logRecords.add(new LogEntry(epochId, node, LogEntry.Role.abdication));
                if (logRecords.size() > MAX_COUNT) {
                    context.stateMachine().shutdown();
                }
            }

            @Override
            public void error(StateMachineContext context, Throwable e) {
                Utils.println("state machine error: node " +
                              context.node() + " message " + e.getMessage());
            }
        };

        final List<ClusterRole> clusterRoleLogs = Collections.synchronizedList(
                new ArrayList<>(100));

        final ClusterRoleStore clusterRoleStore = new ClusterRoleStore() {

            volatile int epoch = 0;

            final Map<Integer, ClusterRole> data = new ConcurrentHashMap<>();

            ClusterRole copy(ClusterRole clusterRole) {
                if (clusterRole == null) {
                    return null;
                }
                return new ClusterRole(clusterRole.node(), clusterRole.url(),
                                       clusterRole.epoch(), clusterRole.clock());
            }

            @Override
            public boolean updateIfNodePresent(ClusterRole clusterRole) {
                if (clusterRole.epoch() < this.epoch) {
                    return false;
                }

                ClusterRole copy = this.copy(clusterRole);
                ClusterRole newClusterRole = this.data.compute(copy.epoch(), (key, value) -> {
                    if (copy.epoch() > this.epoch) {
                        this.epoch = copy.epoch();
                        Assert.assertNull(value);
                        clusterRoleLogs.add(copy);
                        Utils.println("The node " + copy + " become new master:");
                        return copy;
                    }

                    Assert.assertEquals(value.epoch(), copy.epoch());
                    if (Objects.equals(value.node(), copy.node()) &&
                        value.clock() <= copy.clock()) {
                        Utils.println("The master node " + copy + " keep heartbeat");
                        clusterRoleLogs.add(copy);
                        if (value.clock() == copy.clock()) {
                            Assert.fail("Clock must increase when same epoch and node id");
                        }
                        return copy;
                    }
                    return value;

                });
                return Objects.equals(newClusterRole, copy);
            }

            @Override
            public Optional<ClusterRole> query() {
                return Optional.ofNullable(this.copy(this.data.get(this.epoch)));
            }
        };

        RoleElectionStateMachine[] machines = new RoleElectionStateMachine[4];
        Thread node1 = new Thread(() -> {
            Config config = new TestConfig("1");
            RoleElectionStateMachine stateMachine =
                    new StandardRoleElectionStateMachine(config, clusterRoleStore);
            machines[1] = stateMachine;
            stateMachine.start(callback);
            stop.countDown();
        });

        Thread node2 = new Thread(() -> {
            Config config = new TestConfig("2");
            RoleElectionStateMachine stateMachine =
                    new StandardRoleElectionStateMachine(config, clusterRoleStore);
            machines[2] = stateMachine;
            stateMachine.start(callback);
            stop.countDown();
        });

        Thread node3 = new Thread(() -> {
            Config config = new TestConfig("3");
            RoleElectionStateMachine stateMachine =
                    new StandardRoleElectionStateMachine(config, clusterRoleStore);
            machines[3] = stateMachine;
            stateMachine.start(callback);
            stop.countDown();
        });

        node1.start();
        node2.start();
        node3.start();

        Thread randomShutdown = new Thread(() -> {
            Set<String> dropNodes = new HashSet<>();
            while (dropNodes.size() < 3) {
                LockSupport.parkNanos(5_000_000_000L);
                int size = masterNodes.size();
                if (size < 1) {
                    continue;
                }
                String node = masterNodes.get(size - 1);
                if (dropNodes.contains(node)) {
                    continue;
                }
                machines[Integer.parseInt(node)].shutdown();
                dropNodes.add(node);
                Utils.println("----shutdown machine " + node);
            }
            stop.countDown();
        });

        randomShutdown.start();
        stop.await();

        Assert.assertGt(0, logRecords.size());
        Map<Integer, String> masters = new HashMap<>();
        for (LogEntry entry : logRecords) {
            if (entry.role == LogEntry.Role.master) {
                String lastNode = masters.putIfAbsent(entry.epoch, entry.node);
                if (lastNode != null) {
                    Assert.assertEquals(lastNode, entry.node);
                }
            }
        }

        Assert.assertGt(0, masters.size());
    }
}
