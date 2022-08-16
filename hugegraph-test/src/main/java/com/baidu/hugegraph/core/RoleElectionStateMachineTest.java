package com.baidu.hugegraph.core;

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

import org.junit.Assert;
import org.junit.Test;

import com.baidu.hugegraph.election.Config;
import com.baidu.hugegraph.election.MetaData;
import com.baidu.hugegraph.election.MetaDataAdapter;
import com.baidu.hugegraph.election.RoleElectionStateMachine;
import com.baidu.hugegraph.election.RoleElectionStateMachineImpl;
import com.baidu.hugegraph.election.StateMachineCallback;
import com.baidu.hugegraph.election.StateMachineContext;

public class RoleElectionStateMachineTest {

    public static class LogEntry {

        Integer epoch;
        String node;

        Role role;

        enum Role {
            master,
            worker,
            candidate,
            safe,
            unknown
        }

        public LogEntry(Integer epoch, String node, Role role) {
            this.epoch = epoch;
            this.node = node;
            this.role = role;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof LogEntry)) return false;
            LogEntry logEntry = (LogEntry) o;
            return Objects.equals(epoch, logEntry.epoch) && Objects.equals(node, logEntry.node) && role == logEntry.role;
        }

        @Override
        public int hashCode() {
            return Objects.hash(epoch, node, role);
        }

        @Override
        public String toString() {
            return "LogEntry{" +
                    "epoch=" + epoch +
                    ", node='" + node + '\'' +
                    ", role=" + role +
                    '}';
        }
    }

    private static class TestConfig implements Config {

        String node;

        public TestConfig(String node) {
            this.node = node;
        }

        @Override
        public String node() {
            return this.node;
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
        public int exceedsWorkerCount() {
            return 5;
        }

        @Override
        public long baseTimeoutMillisecond() {
            return 100;
        }
    }

    @Test
    public void testStateMachine() throws InterruptedException {
        final CountDownLatch stop = new CountDownLatch(4);
        final int MAX_COUNT = 200;
        final List<LogEntry> logRecords = Collections.synchronizedList(new ArrayList<>(MAX_COUNT));
        final List<String> masterNodes = Collections.synchronizedList(new ArrayList<>(MAX_COUNT));
        final StateMachineCallback callback = new StateMachineCallback() {

            @Override
            public void master(StateMachineContext context) {
                Integer epochId = context.epoch();
                String node = context.node();
                logRecords.add(new LogEntry(epochId, node, LogEntry.Role.master));
                if (logRecords.size() > MAX_COUNT) {
                    context.stateMachine().shutdown();
                }
                System.out.println("----master " + node);
                masterNodes.add(node);
            }

            @Override
            public void worker(StateMachineContext context) {
                Integer epochId = context.epoch();
                String node = context.node();
                logRecords.add(new LogEntry(epochId, node, LogEntry.Role.worker));
                if (logRecords.size() > MAX_COUNT) {
                    context.stateMachine().shutdown();
                }
            }

            @Override
            public void candidate(StateMachineContext context) {
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
            public void safe(StateMachineContext context) {
                Integer epochId = context.epoch();
                String node = context.node();
                logRecords.add(new LogEntry(epochId, node, LogEntry.Role.safe));
                if (logRecords.size() > MAX_COUNT) {
                    context.stateMachine().shutdown();
                }
            }

            @Override
            public void error(StateMachineContext context, Throwable e) {
                System.out.println("----" + context.node() + " " + e.getMessage());
            }
        };

        final List<MetaData> metaDataLogs = Collections.synchronizedList(new ArrayList<>(100));
        final MetaDataAdapter adapter = new MetaDataAdapter() {
            volatile int epoch = 0;
            Map<Integer, MetaData> data = new ConcurrentHashMap<>();

            MetaData copy(MetaData metaData) {
                if (metaData == null) {
                    return null;
                }
                return new MetaData(metaData.node(), metaData.epoch(), metaData.count());
            }
            @Override
            public boolean postDelyIfPresent(MetaData metaData, long delySecond) {
                if (delySecond > 0) {
                    LockSupport.parkNanos(delySecond * 1_000_000_000);
                }
                if (metaData.epoch() < this.epoch) {
                    return false;
                }

                MetaData copy = this.copy(metaData);
                MetaData newData = data.compute(copy.epoch(), (key, value) -> {
                    if (copy.epoch() > this.epoch) {
                        this.epoch = copy.epoch();
                        Assert.assertNull(value);
                        metaDataLogs.add(copy);
                        System.out.println("----1" + copy);
                        return copy;
                    }

                    Assert.assertEquals(value.epoch(), copy.epoch());
                    if (Objects.equals(value.node(), copy.node()) &&
                        value.count() <= copy.count()) {
                        System.out.println("----2" + copy);
                        metaDataLogs.add(copy);
                        if (value.count() == copy.count()) {
                            Exception e = new Exception("eq");
                            e.printStackTrace();
                        }
                        return copy;
                    }
                    return value;

                });
                return Objects.equals(newData, copy);
            }

            @Override
            public Optional<MetaData> queryDelay(long delySecond) {
                LockSupport.parkNanos(delySecond * 1_000_000_000);
                return Optional.ofNullable(this.copy(this.data.get(this.epoch)));
            }

            @Override
            public Optional<MetaData> query() {
                return Optional.ofNullable(this.copy(this.data.get(this.epoch)));
            }
        };

        RoleElectionStateMachine[] machines = new RoleElectionStateMachine[4];
        Thread node1 = new Thread(() -> {
            Config config = new TestConfig("1");
            RoleElectionStateMachine stateMachine = new RoleElectionStateMachineImpl(config, adapter);
            machines[1] = stateMachine;
            stateMachine.apply(callback);
            stop.countDown();
        });

        Thread node2 = new Thread(() -> {
            Config config = new TestConfig("2");
            RoleElectionStateMachine stateMachine = new RoleElectionStateMachineImpl(config, adapter);
            machines[2] = stateMachine;
            stateMachine.apply(callback);
            stop.countDown();
        });

        Thread node3 = new Thread(() -> {
            Config config = new TestConfig("3");
            RoleElectionStateMachine stateMachine = new RoleElectionStateMachineImpl(config, adapter);
            machines[3] = stateMachine;
            stateMachine.apply(callback);
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
                System.out.println("----shutdown machine " + node);
            }
            stop.countDown();
        });

        randomShutdown.start();
        stop.await();

        Assert.assertTrue(logRecords.size() > 0);
        Map<Integer, String> masters = new HashMap<>();
        for (LogEntry entry: logRecords) {
            if (entry.role == LogEntry.Role.master) {
                String lastNode = masters.putIfAbsent(entry.epoch, entry.node);
                if (lastNode != null) {
                    Assert.assertEquals(lastNode, entry.node);
                }
            }
        }

        Assert.assertTrue(masters.size() > 0);
    }
}
