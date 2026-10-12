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

package org.apache.hugegraph.store.core.store;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.SessionOperator;
import org.apache.hugegraph.store.HgStoreEngine;
import org.apache.hugegraph.store.PartitionEngine;
import org.apache.hugegraph.store.business.BusinessHandlerImpl;
import org.apache.hugegraph.store.meta.PartitionManager;
import org.apache.hugegraph.store.meta.ShardGroup;
import org.apache.hugegraph.store.meta.StoreMetadata;
import org.apache.hugegraph.store.options.HgStoreEngineOptions;
import org.apache.hugegraph.store.options.PartitionEngineOptions;
import org.apache.hugegraph.store.raft.PartitionStateMachine;
import org.apache.hugegraph.store.pd.PdProvider;
import org.apache.hugegraph.store.snapshot.SnapshotHandler;
import org.apache.hugegraph.store.term.Bits;
import org.apache.hugegraph.store.util.HgStoreException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;

import com.google.protobuf.Int64Value;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ClosureQueueImpl;
import com.alipay.sofa.jraft.closure.LoadSnapshotClosure;
import com.alipay.sofa.jraft.core.FSMCallerImpl;
import com.alipay.sofa.jraft.core.NodeImpl;
import com.alipay.sofa.jraft.core.NodeMetrics;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.RaftOutter;
import com.alipay.sofa.jraft.option.FSMCallerOptions;
import com.alipay.sofa.jraft.storage.LogManager;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;

public class GraphIdUpgradePreflightTest {

    private static final int PARTITION_ID = 0;
    private static final String DB_NAME = "00000";
    private static final String[] TABLES = {"g+v", "g+oe", "g+ie", "g+index"};
    private static final byte[] VALUE = {42};

    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();

    @BeforeClass
    public static void initRocksdb() {
        Map<String, Object> config = new HashMap<>();
        config.put("rocksdb.write_buffer_size", "1048576");
        config.put("rocksdb.bloom_filter_bits_per_key", "10");
        BusinessHandlerImpl.initRocksdb(config, null);
    }

    @Test
    public void testLegacyDataRejectsBeforeRaftAndPreservesVersions() throws Exception {
        Path location = this.temporary.newFolder("existing-location").toPath();
        Path data = Files.createDirectory(location.resolve("db"));
        writeDatabase(data, DB_NAME, 1);
        writeDatabase(data, DB_NAME + "_4", 65534);
        Map<String, String> before = fileHashes(location);
        PartitionEngine engine = partitionEngine(location, true);
        PartitionEngineOptions options = new PartitionEngineOptions();
        options.setGroupId(PARTITION_ID);

        HgStoreException failure = assertThrows(HgStoreException.class, () -> engine.init(options));

        assertTrue(failure.getMessage().contains("GraphID"));
        assertEquals(false, field(engine, "started"));
        assertNull(engine.getRaftNode());
        assertNull(field(engine, "stateMachine"));
        assertNull(engine.getTaskManager());
        assertEquals(before, fileHashes(location));
    }

    @Test
    public void testMissingLocationChecksAllConfiguredRootsWithoutWriting() throws Exception {
        Path first = this.temporary.newFolder("first-root").toPath();
        Path second = this.temporary.newFolder("second-root").toPath();
        writeDatabase(Files.createDirectory(first.resolve("db")), DB_NAME, 1);
        writeDatabase(Files.createDirectory(second.resolve("db")), DB_NAME, 65534);
        Map<String, String> firstBefore = fileHashes(first);
        Map<String, String> secondBefore = fileHashes(second);
        PartitionEngine engine = partitionEngine(first, false);
        engine.getStoreEngine().getOption().setDataPath(first + "," + second);
        PartitionEngineOptions options = new PartitionEngineOptions();
        options.setGroupId(PARTITION_ID);

        assertThrows(HgStoreException.class, () -> engine.init(options));

        assertNull(engine.getRaftNode());
        assertEquals(firstBefore, fileHashes(first));
        assertEquals(secondBefore, fileHashes(second));
    }

    @Test
    public void testLegacySnapshotKeepsActiveAndTargetData() throws Exception {
        Path data = this.temporary.newFolder("active-parent").toPath();
        try (RocksDBSession active = session(data, "active")) {
            putRows(active, 1);
            writeDatabase(data, "active_5", 2);
            Path snapshot = snapshot(65534);
            Map<String, String> before = fileHashes(data);
            Map<String, String> incomingBefore = fileHashes(snapshot);
            BusinessHandlerImpl handler = handler(active);
            assertEquals(1, handler.getKeyCreator().getGraphId(PARTITION_ID, "test"));
            Map<?, ?> cached = (Map<?, ?>) field(handler.getKeyCreator(), "graphIdCache");
            Object owner = cached.get(PARTITION_ID);

            HgStoreException failure = assertThrows(HgStoreException.class,
                    () -> handler.loadSnapshot(snapshot.toString(), "", PARTITION_ID, 5L));

            assertTrue(failure.getMessage().contains("GraphID"));
            assertNotNull(failure.getCause());
            assertSame(owner, cached.get(PARTITION_ID));
            assertEquals(before, fileHashes(data));
            assertEquals(incomingBefore, fileHashes(snapshot));
            assertTrue(Files.isDirectory(data.resolve("active_5_temp_")));
            assertRows(active, 1);
            assertArrayEquals(Int64Value.of(1L).toByteArray(), active.sessionOp().get("default",
                    "HUGEGRAPH/GRAPH_ID/test".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        }
    }

    @Test
    public void testRejectedNativeSnapshotDoesNotPublishEitherIndex() throws Exception {
        assertSnapshotIndices(true);
    }

    @Test
    public void testAcceptedNativeSnapshotPublishesBothIndices() throws Exception {
        assertSnapshotIndices(false);
    }

    private void assertSnapshotIndices(boolean legacy) throws Exception {
        Path data = this.temporary.newFolder("fsm-active").toPath();
        try (RocksDBSession active = session(data, "fsm")) {
            putRows(active, 1);
            Path snapshot = snapshot(legacy ? 65534 : 2);
            Path snapshotRoot = snapshot.getParent();
            BusinessHandlerImpl handler = handler(active);
            HgStoreEngine store = mock(HgStoreEngine.class);
            PartitionEngine engine = mock(PartitionEngine.class);
            when(engine.getStoreEngine()).thenReturn(store);
            when(engine.getGroupId()).thenReturn(PARTITION_ID);
            when(engine.loadPartitionsFromLocalDb()).thenReturn(Collections.emptyList());
            when(store.getBusinessHandler()).thenReturn(handler);
            PartitionStateMachine machine = new PartitionStateMachine(PARTITION_ID,
                                                                       new SnapshotHandler(engine));
            Field committed = PartitionStateMachine.class.getDeclaredField("committedIndex");
            committed.setAccessible(true);
            committed.setLong(machine, 3L);
            SnapshotReader reader = mock(SnapshotReader.class);
            when(reader.getPath()).thenReturn(snapshotRoot.toString());
            when(reader.load()).thenReturn(RaftOutter.SnapshotMeta.newBuilder()
                                                    .setLastIncludedIndex(5L)
                                                    .setLastIncludedTerm(1L).build());
            NodeImpl node = mock(NodeImpl.class);
            when(node.getNodeMetrics()).thenReturn(new NodeMetrics(false));
            when(node.getGroupId()).thenReturn("graph-id-snapshot-test");
            FSMCallerOptions options = new FSMCallerOptions();
            options.setNode(node);
            options.setFsm(machine);
            options.setLogManager(mock(LogManager.class));
            options.setClosureQueue(new ClosureQueueImpl("graph-id-snapshot-test"));
            options.setBootstrapId(new LogId(3L, 1L));
            options.setDisruptorBufferSize(16);
            FSMCallerImpl caller = new FSMCallerImpl();
            assertTrue(caller.init(options));
            AtomicReference<Status> result = new AtomicReference<>();
            CountDownLatch done = new CountDownLatch(1);
            try {
                assertTrue(caller.onSnapshotLoad(new LoadSnapshotClosure() {
                    @Override
                    public SnapshotReader start() {
                        return reader;
                    }

                    @Override
                    public void run(Status status) {
                        result.set(status);
                        done.countDown();
                    }
                }));
                assertTrue(done.await(5, TimeUnit.SECONDS));
                assertEquals(!legacy, result.get().isOk());
                assertEquals(legacy ? 3L : 5L, machine.getCommittedIndex());
                assertEquals(legacy ? 3L : 5L, caller.getLastAppliedIndex());
                assertEquals(legacy ? 3L : 5L, caller.getLastCommittedIndex());
                assertEquals(!legacy, Files.exists(snapshotRoot.resolve("should_not_load")));
                if (legacy) {
                    assertRows(active, 1);
                    assertFalse(Files.exists(data.resolve("fsm_5")));
                } else {
                    assertTrue(Files.isDirectory(data.resolve("fsm_5")));
                    try (RocksDBSession restored = session(data, "fsm")) {
                        assertRows(restored, 2);
                        BusinessHandlerImpl restoredHandler = handler(restored);
                        assertEquals(2, restoredHandler.getKeyCreator().getGraphId(PARTITION_ID, "test"));
                        for (String table : TABLES) {
                            assertArrayEquals(VALUE, restoredHandler.doGet("test", 0, table, new byte[]{1}));
                        }
                    }
                }
            } finally {
                caller.shutdown();
                caller.join();
            }
        }
    }

    private PartitionEngine partitionEngine(Path location, boolean persistedLocation) {
        HgStoreEngine store = mock(HgStoreEngine.class);
        PartitionManager manager = mock(PartitionManager.class);
        StoreMetadata metadata = mock(StoreMetadata.class);
        HgStoreEngineOptions options = new HgStoreEngineOptions();
        options.setDataPath(location.toString());
        when(store.getOption()).thenReturn(options);
        when(store.getPartitionManager()).thenReturn(manager);
        when(manager.getStoreMetadata()).thenReturn(metadata);
        if (persistedLocation) {
            when(metadata.getPartitionStore(PARTITION_ID)).thenReturn(
                    Metapb.PartitionStore.newBuilder().setPartitionId(PARTITION_ID)
                          .setStoreLocation(location.toString()).build());
        }
        return new PartitionEngine(store, mock(ShardGroup.class));
    }

    private static BusinessHandlerImpl handler(RocksDBSession active) {
        PartitionManager manager = mock(PartitionManager.class);
        PdProvider provider = mock(PdProvider.class);
        when(manager.getPdProvider()).thenReturn(provider);
        when(manager.hasPartition(anyString(), anyInt())).thenReturn(true);
        when(provider.getPartitionByCode(anyString(), anyInt())).thenReturn(
                Metapb.Partition.newBuilder().setId(PARTITION_ID).build());
        return new BusinessHandlerImpl(manager) {
            @Override
            public RocksDBSession getSession(int partitionId) {
                return active.clone();
            }
        };
    }

    private Path snapshot(int graphId) throws Exception {
        Path seed = this.temporary.newFolder("snapshot-seed").toPath();
        Path root = this.temporary.newFolder("snapshot-root").toPath();
        Path data = root.resolve("data");
        try (RocksDBSession session = session(seed, "seed")) {
            putRows(session, graphId);
            try (Checkpoint checkpoint = Checkpoint.create(session.getDB())) {
                checkpoint.createCheckpoint(data.toString());
            }
        }
        return data;
    }

    private static void writeDatabase(Path root, String name, int graphId) {
        List<ColumnFamilyHandle> handles = new ArrayList<>();
        try (DBOptions options = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
             ColumnFamilyOptions cfOptions = new ColumnFamilyOptions().setWriteBufferSize(1048576)) {
            List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
            descriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions));
            for (String table : TABLES) {
                descriptors.add(new ColumnFamilyDescriptor(table.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                                                            cfOptions));
            }
            try (RocksDB database = RocksDB.open(options, root.resolve(name).toString(), descriptors, handles)) {
                for (int i = 1; i < handles.size(); i++) {
                    database.put(handles.get(i), key(graphId), VALUE);
                }
            } finally {
                handles.forEach(ColumnFamilyHandle::close);
            }
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }

    private static Object field(Object object, String name) throws Exception {
        Field field = object.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(object);
    }

    private static RocksDBSession session(Path root, String name) {
        return new RocksDBSession(org.apache.hugegraph.rocksdb.access.RocksDBFactory
                                .getInstance().getHugeConfig(), root.toString(), name, 0L);
    }

    private static void putRows(RocksDBSession session, int graphId) {
        session.createTables(TABLES);
        SessionOperator operator = session.sessionOp();
        operator.prepare();
        try {
            for (String table : TABLES) {
                operator.put(table, key(graphId), VALUE);
            }
            if (graphId != 65534) {
                operator.put("default", "HUGEGRAPH/GRAPH_ID/test".getBytes(java.nio.charset.StandardCharsets.UTF_8),
                             Int64Value.of(graphId).toByteArray());
            }
            operator.commit();
        } catch (RuntimeException failure) {
            operator.rollback();
            throw failure;
        }
    }

    private static void assertRows(RocksDBSession session, int graphId) {
        for (String table : TABLES) {
            assertArrayEquals(VALUE, session.sessionOp().get(table, key(graphId)));
        }
    }

    private static byte[] key(int graphId) {
        byte[] key = {0, 0, 1, 0, 0};
        Bits.putShort(key, 0, graphId);
        return key;
    }

    private static Map<String, String> fileHashes(Path root) throws Exception {
        Map<String, String> hashes = new LinkedHashMap<>();
        try (Stream<Path> paths = Files.walk(root)) {
            for (Path path : (Iterable<Path>) paths.filter(Files::isRegularFile)
                                                   .sorted()::iterator) {
                // Rejected loads deliberately retain a new staging directory.
                if (path.toString().contains("_temp_")) {
                    continue;
                }
                hashes.put(root.relativize(path).toString(),
                           Arrays.toString(MessageDigest.getInstance("SHA-256")
                                                        .digest(Files.readAllBytes(path))));
            }
        }
        return hashes;
    }
}
