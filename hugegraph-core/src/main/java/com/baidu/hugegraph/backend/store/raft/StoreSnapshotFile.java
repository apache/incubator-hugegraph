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

package com.baidu.hugegraph.backend.store.raft;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.zip.Checksum;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.LocalFileMetaOutter.LocalFileMeta;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.util.CRC64;
import com.baidu.hugegraph.testutil.Whitebox;
import com.baidu.hugegraph.util.CompressUtil;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.InsertionOrderUtil;
import com.baidu.hugegraph.util.Log;
import com.google.protobuf.ByteString;

public class StoreSnapshotFile {

    private static final Logger LOG = Log.logger(StoreSnapshotFile.class);

    public static final String SNAPSHOT_DIR = "snapshot";
    private static final String TAR = ".tar";

    private final RaftBackendStore[] stores;
    private final Map<String, String> dataDisks;

    public StoreSnapshotFile(RaftBackendStore[] stores) {
        this.stores = stores;
        this.dataDisks = new HashMap<>();
        for (RaftBackendStore raftStore : stores) {
            // Call RocksDBStore method reportDiskMapping()
            this.dataDisks.putAll(Whitebox.invoke(raftStore, "store",
                                                  "reportDiskMapping"));
        }
        /*
         * Like that:
         * general=/parent_path/rocksdb-data
         * g/VERTEX=/parent_path/rocksdb-vertex
         */
        LOG.debug("The store data disks mapping {}", this.dataDisks);
    }

    public void save(SnapshotWriter writer, Closure done,
                     ExecutorService executor) {
        try {
            // Write snapshot to real directory
            Map<String, String> snapshotDirMaps = this.doSnapshotSave();
            executor.execute(() -> {
                try {
                    this.compressSnapshotDir(writer, snapshotDirMaps);
                    this.deleteSnapshotDirs(snapshotDirMaps.keySet());
                    done.run(Status.OK());
                } catch (Throwable e) {
                    LOG.error("Failed to compress snapshot", e);
                    done.run(new Status(RaftError.EIO,
                                        "Failed to compress snapshot, " +
                                        "error is %s", e.getMessage()));
                }
            });
        } catch (Throwable e) {
            LOG.error("Failed to save snapshot", e);
            done.run(new Status(RaftError.EIO,
                                "Failed to save snapshot, error is %s",
                                e.getMessage()));
        }
    }

    public boolean load(SnapshotReader reader) {
        Set<String> snapshotDirTars = reader.listFiles();
        LOG.info("The snapshot tar files to be loaded are {}", snapshotDirTars);
        Set<String> snapshotDirs = new HashSet<>();
        for (String snapshotDirTar : snapshotDirTars) {
            try {
                String snapshotDir = this.decompressSnapshot(reader,
                                                             snapshotDirTar);
                snapshotDirs.add(snapshotDir);
            } catch (Throwable e) {
                LOG.error("Failed to decompress snapshot tar", e);
                return false;
            }
        }

        try {
            this.doSnapshotLoad();
            this.deleteSnapshotDirs(snapshotDirs);
        } catch (Throwable e) {
            LOG.error("Failed to load snapshot", e);
            return false;
        }
        return true;
    }

    private Map<String, String> doSnapshotSave() {
        Map<String, String> snapshotDirMaps = InsertionOrderUtil.newMap();
        for (RaftBackendStore store : this.stores) {
            snapshotDirMaps.putAll(store.originStore()
                                        .createSnapshot(SNAPSHOT_DIR));
        }
        LOG.info("Saved all snapshots: {}", snapshotDirMaps);
        return snapshotDirMaps;
    }

    private void doSnapshotLoad() {
        for (RaftBackendStore store : this.stores) {
            store.originStore().resumeSnapshot(SNAPSHOT_DIR, false);
        }
    }

    private void compressSnapshotDir(SnapshotWriter writer,
                                     Map<String, String> snapshotDirMaps) {
        String writerPath = writer.getPath();
        for (Map.Entry<String, String> entry : snapshotDirMaps.entrySet()) {
            String snapshotDir = entry.getKey();
            String diskTableKey = entry.getValue();
            String snapshotDirTar = Paths.get(snapshotDir).getFileName()
                                         .toString() + TAR;
            String outputFile = Paths.get(writerPath, snapshotDirTar)
                                     .toString();
            Checksum checksum = new CRC64();
            try {
                CompressUtil.compressTar(snapshotDir, outputFile, checksum);
            } catch (Throwable e) {
                throw new RaftException(
                          "Failed to compress snapshot, path=%s, files=%s",
                          e, writerPath, snapshotDirMaps.keySet());
            }

            LocalFileMeta.Builder metaBuilder = LocalFileMeta.newBuilder();
            metaBuilder.setChecksum(Long.toHexString(checksum.getValue()));
            /*
             * snapshot_rocksdb-data.tar -> general
             * snapshot_rocksdb-vertex.tar -> g/VERTEX
             */
            metaBuilder.setUserMeta(ByteString.copyFromUtf8(diskTableKey));
            if (!writer.addFile(snapshotDirTar, metaBuilder.build())) {
                throw new RaftException("Failed to add snapshot file: '%s'",
                                        snapshotDirTar);
            }
        }
    }

    private String decompressSnapshot(SnapshotReader reader,
                                      String snapshotDirTar)
                                      throws IOException {
        LocalFileMeta meta = (LocalFileMeta) reader.getFileMeta(snapshotDirTar);
        if (meta == null) {
            throw new IOException("Can't find snapshot archive file, path=" +
                                  snapshotDirTar);
        }

        String diskTableKey = meta.getUserMeta().toStringUtf8();
        E.checkArgument(this.dataDisks.containsKey(diskTableKey),
                        "The data path for '%s' should be exist", diskTableKey);
        String dataPath = this.dataDisks.get(diskTableKey);
        String parentPath = Paths.get(dataPath).getParent().toString();
        String snapshotDir = Paths.get(parentPath,
                                       StringUtils.removeEnd(snapshotDirTar, TAR))
                                  .toString();
        FileUtils.deleteDirectory(new File(snapshotDir));
        LOG.info("Delete stale snapshot dir {}", snapshotDir);

        Checksum checksum = new CRC64();
        String archiveFile = Paths.get(reader.getPath(), snapshotDirTar)
                                  .toString();
        CompressUtil.decompressTar(archiveFile, parentPath, checksum);
        if (meta.hasChecksum()) {
            String expected = meta.getChecksum();
            String actual = Long.toHexString(checksum.getValue());
            E.checkArgument(expected.equals(actual),
                            "Snapshot checksum error: '%s' != '%s'",
                            actual, expected);
        }
        return snapshotDir;
    }

    private void deleteSnapshotDirs(Set<String> snapshotDirs) {
        for (String snapshotDir : snapshotDirs) {
            FileUtils.deleteQuietly(new File(snapshotDir));
        }
    }
}
