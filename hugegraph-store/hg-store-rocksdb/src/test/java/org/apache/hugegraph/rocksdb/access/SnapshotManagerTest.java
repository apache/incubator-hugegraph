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

package org.apache.hugegraph.rocksdb.access;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.hugegraph.store.term.HgPair;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SnapshotManagerTest {

    final String tempSuffix = "_temp_";
    final String graphName = "test_graph";

    // @Test
    public void testRetrieveIndex() throws DBStoreException {
        String snapshotPath =
                "D:\\Code\\baidu\\starhugegraph\\hugegraph-store\\tmp\\8501\\raft\\1/snapshot" +
                "\\snapshot_40\\graph_1_p1";
        long lastIndex = 0L;
        File file = new File(snapshotPath);
        File parentFile = new File(file.getParent());
        String[] arr = parentFile.getName().split("_");
        if (arr.length >= 2) {
            lastIndex = Long.parseLong(arr[arr.length - 1]);
        } else {
            throw new DBStoreException(String.format("Invalid Snapshot path %s", snapshotPath));
        }

        System.out.println(lastIndex);
    }

    // @Test
    public void testFoundMaxDir() {
//        String path = "D:\\Code\\baidu\\starhugegraph\\hugegraph-store\\tmp\\8503\\db\\default
//        \\hugegraph\\g\\0_123";
//        String path = "D:\\Code\\baidu\\starhugegraph\\hugegraph-store\\tmp\\8503\\db\\default
//        \\hugegraph\\g\\0";
//        String path = "D:\\Code\\baidu\\starhugegraph\\hugegraph-store\\tmp\\8501\\db\\default
//        \\hugegraph\\g\\0";
//        String path = "D:\\tmp\\db\\0";
//        String path = "D:\\tmp\\db\\0_111";
        String path = "D:\\tmp\\db\\0_111_22222";
        File file = new File(path);
        int strIndex = file.getName().indexOf("_");

        String defaultName;
        if (strIndex < 0) {
            defaultName = file.getName();
        } else {
            defaultName = file.getName().substring(0, strIndex);
        }
        String prefix = defaultName + "_";
        File parentFile = new File(file.getParent());
        final List<HgPair<Long, Long>> dbs = new ArrayList<>();
        final File[] files = parentFile.listFiles();
        if (files != null) {
            // search all db path
            for (final File sFile : files) {
                final String name = sFile.getName();
                if (!name.startsWith(prefix) && !name.equals(defaultName)) {
                    continue;
                }
                if (name.endsWith(tempSuffix)) {
                    continue;
                }
                long v1 = -1L;
                long v2 = -1L;
                if (name.length() > defaultName.length()) {
                    String[] versions = name.substring(prefix.length()).split("_");
                    if (versions.length == 1) {
                        v1 = Long.parseLong(versions[0]);
                    } else if (versions.length == 2) {
                        v1 = Long.parseLong(versions[0]);
                        v2 = Long.parseLong(versions[1]);
                    } else {
                        continue;
                    }
                }
                dbs.add(new HgPair<>(v1, v2));
            }
        }

        // get last index db path
        String latestDBPath = "";
        if (!dbs.isEmpty()) {
            dbs.sort((o1, o2) -> o1.getKey().equals(o2.getKey()) ?
                                 o1.getValue().compareTo(o2.getValue()) :
                                 o1.getKey().compareTo(o2.getKey()));
            final int dbCount = dbs.size();

            // delete old db
            for (int i = 0; i < dbCount; i++) {
                final HgPair<Long, Long> pair = dbs.get(i);
                String curDBName;
                if (pair.getKey() == -1L) {
                    curDBName = defaultName;
                } else if (pair.getValue() == -1L) {
                    curDBName = String.format("%s_%d", defaultName, pair.getKey());
                } else {
                    curDBName =
                            String.format("%s_%d_%d", defaultName, pair.getKey(), pair.getValue());
                }
                String curDBPath = Paths.get(parentFile.getPath(), curDBName).toString();
                if (i == dbCount - 1) {
                    latestDBPath = curDBPath;
                } else {
                    log.info("delete old dbpath {}", curDBPath);
                }
            }

        } else {
            latestDBPath = Paths.get(parentFile.getPath(), defaultName).toString();
        }
        log.info("{} latest db path {}", this.graphName, latestDBPath);

    }

    // @Test
    public void testDefaultPath() {
//        String latestDBPath = "D:\\Code\\baidu\\starhugegraph\\hugegraph-store\\tmp\\8501\\db
//        \\default\\hugegraph\\g\\0_123";
        String latestDBPath =
                "D:\\Code\\baidu\\starhugegraph\\hugegraph-store\\tmp\\8501\\db\\default" +
                "\\hugegraph\\g\\0";
        File file = new File(latestDBPath);
        String parent = file.getParent();
        String defaultName = file.getName().split("_")[0];
        String defaultPath = Paths.get(parent, defaultName).toString();
        System.out.println(defaultPath);
    }

//    private static final String graphName = "unit-test-graph";
//    private static final String graphName2 = "unit-test-graph-2";
//    private static final int partition_1 = 1;
//    private static final int partition_2 = 2;
//    private static final int partition_3 = 3;
//    private static final int partition_4 = 4;
//    private static ArrayList<String> tableList = new ArrayList<String>();
//    private static ArrayList<Integer> partitionList = new ArrayList<Integer>();
//    private static RocksDBFactory factory = RocksDBFactory.getInstance();
//    private static HugeConfig hConfig;
//    private static HugeConfig hConfig2;
//    private int index = 0;
//
//    private static byte[] values = new byte[1024];
//
//
//    // @BeforeClass
//    public static void beforeClass() {
//        // Register config
//        OptionSpace.register("rocksdb",
//                "org.apache.hugegraph.rocksdb.access.RocksDBOptions");
//        // Register backend
//        URL configPath = SnapshotManagerTest.class.getResource("/hugegraph.properties");
//        URL configPath2 = SnapshotManagerTest.class.getResource("/hugegraph-2.properties");
//        hConfig = new HugeConfig(configPath.getPath());
//        hConfig2 = new HugeConfig(configPath2.getPath());
//        factory.setHugeConfig(hConfig);
//        // init tables
//        tableList.add("t1");
//        tableList.add("t2");
//        tableList.add("t3");
//        tableList.add("t4");
//        tableList.add("abc/t5");
//        tableList.add("../t6");
//        // init partitons
//        partitionList.add(partition_1);
//        partitionList.add(partition_2);
//        partitionList.add(partition_3);
//        partitionList.add(partition_4);
//
//        for (int i = 0; i < 1024; i++)
//            values[i] = (byte) (i % 0x7F);
//
//    }
//
//    @AfterClass
//    public static void afterClass() {
//        factory.releaseAllGraphDB();
//    }
//    // @Test
//    public void test(){
//        Map<String, Object> configMap = new HashMap<>();
//        configMap.put("rocksdb.data_path", "tmp/test2/huge/");
//        configMap.put("rocksdb.wal_path", "tmp/test2/wal");
//        configMap.put("rocksdb.snapshot_path", "tmp/test2/snapshot");
//        configMap.put("rocksdb.write_buffer_size","1048576");
//        HugeConfig config = new HugeConfig(configMap);
//        factory.setHugeConfig(config);
//        RocksDBSession session = factory.createGraphDB(graphName, false);
//        SessionOperator sessionOp = session.sessionOp();
//
//        String table = "t1";
//
//        String startKey = String.format("%d_%08d", partition_1, 0);
//        String lastKey = String.format("%d_%08d", partition_1, 99);
//        String endKey = String.format("%d_%08d", partition_1, 100);
//
//
//        printKV(sessionOp, table, startKey);
//        printKV(sessionOp, table, lastKey);
//    }
//
//    private void printKV(SessionOperator sessionOp, String table, String key) {
//        byte[] value = sessionOp.get(table, key.getBytes());
//        if (value != null)
//            System.out.printf("table=%s, key=%s, value=%s\n", table, key, new String(value));
//        else
//            System.out.printf("table=%s, key=%s, does not found\n", table, key);
//    }
//
//    private void insertData(int count) {
//        RocksDBSession session = factory.createGraphDB(graphName, false);
//        SessionOperator sessionOp = session.sessionOp();
//        sessionOp.prepare();
//        sessionOp.prepare();
//        final int begin = index;
//        final int end = index + count;
//        System.out.printf("begin to insert data from index %d to %d\n", begin, end);
//
//        for (; index < end; index++) {
//            for (String table : tableList) {
//                for (Integer partition : partitionList) {
//                    String key = String.format("%d_%08d", partition, index);
//                    String value = String.format("%s_%08d", table, index);
//                    sessionOp.put(table, key.getBytes(), value.getBytes());
//                }
//            }
//            if (index % 1000 == 999) {
//                System.out.printf("insertData, commit index %d\n", index+1);
//                sessionOp.commit();
//                sessionOp.prepare();
//            }
//        }
//        sessionOp.commit();
//        session.flush();
//        System.out.printf("inserted %d pairs data\n", count);
//    }
//
//    // @Test
//    public void testSnapshotMetadata() {
//        DBSnapshotMeta metadata = new DBSnapshotMeta();
//        metadata.setGraphName(graphName);
//        metadata.setPartitionId(partition_1);
//        metadata.setStartKey("k1".getBytes());
//        metadata.setEndKey("k9".getBytes());
//        HashMap<String, String> sstFiles = new HashMap<String, String>();
//        sstFiles.put("cf1", "cf1.sst");
//        sstFiles.put("cf2", "cf2.sst");
//        sstFiles.put("cf3", "cf3.sst");
//        sstFiles.put("cf4", "cf4.sst");
//        metadata.setCreatedDate(new Date());
//        metadata.setSstFiles(sstFiles);
//
//        System.out.println("metadata object:");
//        System.out.println(metadata.toString());
//
//        String jsonStr = JSON.toJSONString(metadata);
//        System.out.println("metadata json:");
//        System.out.println(jsonStr);
//
//        DBSnapshotMeta metadata2 = JSON.parseObject(jsonStr, DBSnapshotMeta.class);
//        System.out.println("metadata2 object:");
//        System.out.println(metadata2.toString());
//
//        assertEquals(metadata, metadata2);
//    }
//
//    // @Test
//    public void testCompress() throws IOException {
//        final String rootDir = "./tmp/fileutils";
//        final File dir1 = Paths.get(rootDir, "dir1").toFile();
//        final File dir2 = Paths.get(rootDir, "dir2").toFile();
//        final File dir3 = Paths.get(rootDir, "dir3").toFile();
//        final File dir4 = Paths.get(rootDir, "dir4").toFile();
//        final File file0 = Paths.get(rootDir, "file0").toFile();
//        final File file1 = Paths.get(dir1.getAbsolutePath(), "file1").toFile();
//        FileUtils.forceMkdirParent(dir1);
//        FileUtils.forceMkdirParent(dir2);
//        FileUtils.forceMkdirParent(dir3);
//        FileUtils.deleteDirectory(dir2);
//        FileUtils.forceMkdirParent(dir4);
//        FileUtils.moveDirectory(dir3, dir1);
//        FileUtils.touch(file0);
//        FileUtils.touch(file1);
//        final Checksum c1 = new CRC64();
//        ZipUtils.compress("./tmp", "fileutils", "./tmp/fileutils.zip", c1);
//
//        FileUtils.deleteDirectory(new File("./tmp/fileutils"));
//
//        final Checksum c2 = new CRC64();
//        ZipUtils.decompress("./tmp/fileutils.zip", "./tmp", c2);
//        Assert.assertEquals(c1.getValue(), c2.getValue());
//    }
//
//
//    // @Test
//    public void testDecompress() throws IOException {
//        final Checksum checksum = new CRC64();
//        String dir = "./tmp/server-00/snapshot/test/import/1";
//        String source = Paths.get(dir, "snapshot.zip").toString();
//        FileUtils.deleteDirectory(Paths.get(dir, "snapshot").toFile());
//        ZipUtils.decompress(source, dir, checksum);
//    }
//
//
//    // @Test
//    public void testExportSnapshot() throws DBStoreException, IOException {
//        final String exportDir = hConfig.getString("rocksdb.snapshot_path");
//        File exportDirFile = new File(exportDir);
//        if (exportDirFile.exists()) {
//            FileUtils.forceDelete(new File(exportDir));
//        }
//        FileUtils.forceMkdir(new File(exportDir));
//
//        RocksDBSession session = factory.createGraphDB(graphName, false);
//        insertData(1000);
//        String startKey = String.format("%d_%08d", partition_1, 0);
//        String endKey = String.format("%d_%08d", partition_1, 100);
//        session.exportSnapshot(exportDir, 1, startKey.getBytes(), endKey.getBytes());
//    }
//
//    // @Test
//    public void testImportSnapshot() throws DBStoreException, IOException {
//        final String importDir = hConfig.getString("rocksdb.snapshot_path");
//        FileUtils.forceMkdir(new File(importDir));
//
//        factory.setHugeConfig(hConfig2);
//        RocksDBSession session = factory.createGraphDB(graphName, false);
//        SessionOperator sessionOp = session.sessionOp();
//
//        String table = "t1";
//
//        String startKey = String.format("%d_%08d", partition_1, 0);
//        String lastKey = String.format("%d_%08d", partition_1, 99);
//        String endKey = String.format("%d_%08d", partition_1, 100);
//
//        session.importSnapshot(importDir, partition_1);
//        long keyCount = session.sessionOp().keyCount(table, startKey.getBytes(), endKey
//        .getBytes());
//        System.out.printf("key count of %s is %d\n", graphName, keyCount);
//
//
//        System.out.printf("after load snapshot\n");
//        printKV(sessionOp, table, startKey);
//        printKV(sessionOp, table, lastKey);
//    }
//
//    // @Test
//    public void testExportSnapshot2() throws DBStoreException, IOException {
//        final String exportDir = hConfig.getString("rocksdb.snapshot_path");
//        deleteDir(new File(hConfig.getString("rocksdb.data_path")));
//        deleteDir(new File(exportDir));
//        FileUtils.forceMkdir(new File(exportDir));
//
//        RocksDBSession session = factory.createGraphDB(graphName, false);
//
//        long seqNum = session.getLatestSequenceNumber();
//        System.out.println(seqNum);
//        insertData(500);
//        System.out.println(session.getLatestSequenceNumber());
//        String startKey = String.format("%d_%08d", partition_1, index - 200);
//        String endKey = String.format("%d_%08d", partition_1, index - 100);
//
//        ScanIterator cfIterator = session.sessionOp().scanRaw(startKey.getBytes(), endKey
//        .getBytes(), seqNum);
//        while (cfIterator.hasNext()) {
//            ScanIterator iterator = cfIterator.next();
//            System.out.println("cf name is " + new String(cfIterator.position()));
//            while (iterator.hasNext()) {
//                RocksDBSession.BackendColumn col = iterator.next();
//                System.out.println(new String(col.name));
//            }
//            System.out.println("-----------------------------------------");
//            iterator.close();
//        }
//        cfIterator.close();
//    }
//
//    // @Test
//    public void testImportSnapshot2() throws DBStoreException, IOException {
//        final String importDir = "/tmp/snapshot";
//        deleteDir(new File("/tmp/rocksdb/data" + "/" + graphName2));
//        RocksDBSession session = factory.createGraphDB(graphName2, false);
//        SessionOperator sessionOp = session.sessionOp();
//
//
//        session.flush();
//
//        String table = "t1";
//
//        String k1 = String.format("%d_%08d", partition_1, 100);
//        String k2 = String.format("%d_%08d", partition_1, 109);
//        String k3 = String.format("%d_%08d", partition_1, 110);
//
//        printKV(sessionOp, table, k1);
//        printKV(sessionOp, table, k2);
//        printKV(sessionOp, table, k3);
//
//    }
//
//
//    private static boolean deleteDir(File dir) {
//        if (dir.isDirectory())
//            for (File file : dir.listFiles())
//                deleteDir(file);
//        return dir.delete();
//    }
}
