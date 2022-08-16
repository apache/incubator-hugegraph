package com.baidu.hugegraph.core;

import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;

import com.baidu.hugegraph.election.Config;
import com.baidu.hugegraph.election.MetaData;
import com.baidu.hugegraph.election.MetaDataAdapter;
import com.baidu.hugegraph.election.RoleElectionStateMachine;
import com.baidu.hugegraph.election.RoleElectionStateMachineImpl;
import com.baidu.hugegraph.election.StateMachineCallback;
import com.baidu.hugegraph.election.StateMachineContext;

import org.junit.Assert;
import org.junit.Test;

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
            return 10;
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
    }

    @Test
    public void testStateMachine() throws InterruptedException {
        final CountDownLatch stop = new CountDownLatch(3);
        final int MAX_COUNT = 100;
        final List<LogEntry> logRecords = Collections.synchronizedList(new ArrayList<>(MAX_COUNT));
        final StateMachineCallback callback = new StateMachineCallback() {

            @Override
            public void master(StateMachineContext context) {
                Integer epochId = context.epoch();
                String node = context.node();
                logRecords.add(new LogEntry(epochId, node, LogEntry.Role.master));
                if (logRecords.size() > MAX_COUNT) {
                    context.stateMachine().shutdown();
                }
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

            }
        };
        final MetaDataAdapter adapter = new MetaDataAdapter() {
            int epoch = 0;
            int count = 0;
            Map<Integer, MetaData> data = new ConcurrentHashMap<>();
            @Override
            public boolean postDelyIfPresent(MetaData metaData, long delySecond) {
                this.count ++;
                LockSupport.parkNanos(delySecond * 1_000_000_000);
                if (count > 10) {
                    throw new RuntimeException("timeout");
                }
                MetaData oldData = data.computeIfAbsent(metaData.epoch(), (key) -> {
                    this.epoch = Math.max(key, epoch);
                    return metaData;
                });
                return oldData == metaData;
            }

            @Override
            public Optional<MetaData> queryDelay(long delySecond) {
                LockSupport.parkNanos(delySecond * 1_000_000_000);
                return Optional.ofNullable(this.data.get(this.epoch));
            }

            @Override
            public Optional<MetaData> query() {
                return Optional.ofNullable(this.data.get(this.epoch));
            }
        };

        Thread node1 = new Thread(() -> {
            Config config = new TestConfig("1");
            RoleElectionStateMachine stateMachine = new RoleElectionStateMachineImpl(config, adapter);
            stateMachine.apply(callback);
            stop.countDown();
        });

        Thread node2 = new Thread(() -> {
            Config config = new TestConfig("2");
            RoleElectionStateMachine stateMachine = new RoleElectionStateMachineImpl(config, adapter);
            stateMachine.apply(callback);
            stop.countDown();
        });

        Thread node3 = new Thread(() -> {
            Config config = new TestConfig("3");
            RoleElectionStateMachine stateMachine = new RoleElectionStateMachineImpl(config, adapter);
            stateMachine.apply(callback);
            stop.countDown();
        });

        node1.start();
        node2.start();
        node3.start();

        stop.await();

        Assert.assertTrue(logRecords.size() > MAX_COUNT);
        Map<Integer, String> masters = new HashMap<>();
        for (LogEntry entry: logRecords) {
            if (entry.role == LogEntry.Role.master) {
                String lastNode = masters.putIfAbsent(entry.epoch, entry.node);
                Assert.assertEquals(lastNode, entry.node);
            }
        }

        Assert.assertTrue(masters.size() > 0);
    }
}
