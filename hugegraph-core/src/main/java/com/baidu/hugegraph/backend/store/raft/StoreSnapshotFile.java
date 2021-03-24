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

import static com.alipay.sofa.jraft.entity.LocalFileMetaOutter.LocalFileMeta;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.zip.Checksum;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.util.CRC64;
import com.baidu.hugegraph.util.CompressUtil;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

public class StoreSnapshotFile {

    private static final Logger LOG = Log.logger(StoreSnapshotFile.class);

    public static final String SNAPSHOT_DIR = "snapshot";
    private static final String ARCHIVE_FORMAT = ".tar";
    private static final String SNAPSHOT_ARCHIVE = SNAPSHOT_DIR + ARCHIVE_FORMAT;
    private static final String MANIFEST = "manifest";

    private final RaftBackendStore[] stores;

    public StoreSnapshotFile(RaftBackendStore[] stores) {
        this.stores = stores;
    }

    public void save(SnapshotWriter writer, Closure done,
                     ExecutorService executor) {
        try {
            // Write snapshot to real directory
            Set<String> snapshotDirs = this.doSnapshotSave();
            executor.execute(() -> {
                long begin = System.currentTimeMillis();
                String jraftSnapshotPath =
                            this.writeManifest(writer, snapshotDirs, done);
                this.compressJraftSnapshotDir(writer, jraftSnapshotPath, done);
                LOG.info("Compress snapshot cost {}ms",
                         System.currentTimeMillis() - begin);
            });
        } catch (Throwable t) {
            LOG.error("Failed to save snapshot", t);
            done.run(new Status(RaftError.EIO,
                                "Failed to save snapshot, error is %s",
                                t.getMessage()));
        }
    }

    public boolean load(SnapshotReader reader) {
        LocalFileMeta meta = (LocalFileMeta) reader.getFileMeta(SNAPSHOT_ARCHIVE);
        String readerPath = reader.getPath();
        if (meta == null) {
            LOG.error("Can't find snapshot archive file, path={}", readerPath);
            return false;
        }
        String jraftSnapshotPath = Paths.get(readerPath, SNAPSHOT_DIR)
                                        .toString();
        try {
            // decompress manifest and data directory
            this.decompressSnapshot(readerPath, meta);
            this.doSnapshotLoad();
            File tmp = new File(jraftSnapshotPath);
            // Delete the decompressed temporary file. If the deletion fails
            // (although it is a small probability event), it may affect the
            // next snapshot decompression result. Therefore, the safest way
            // is to terminate the state machine immediately. Users can choose
            // to manually delete and restart according to the log information.
            if (tmp.exists()) {
                FileUtils.forceDelete(tmp);
            }
            return true;
        } catch (Throwable t) {
            LOG.error("Failed to load snapshot", t);
            return false;
        }
    }

    private Set<String> doSnapshotSave() {
        Set<String> snapshotDirs = new HashSet<>();
        for (RaftBackendStore store : this.stores) {
            snapshotDirs.addAll(store.originStore().createSnapshot(SNAPSHOT_DIR));
        }
        LOG.info("All snapshot dirs: {}", snapshotDirs);
        return snapshotDirs;
    }

    private void doSnapshotLoad() {
        for (RaftBackendStore store : this.stores) {
            store.originStore().resumeSnapshot(SNAPSHOT_DIR);
        }
    }

    private Set<String> compressSnapshotDir(Set<String> snapshotDirs,
                                            Closure done) {
        // Compress all backend snapshot dir
        Set<String> tarSnapshotFiles = new HashSet<>();
        for (String snapshotDir : snapshotDirs) {
            String outputFile = snapshotDir + ARCHIVE_FORMAT;
            try {
                CompressUtil.compressTar(snapshotDir, outputFile, new CRC64());
            } catch (IOException e) {
                done.run(new Status(RaftError.EIO,
                                    "Failed to compress backend snapshot dir " +
                                    snapshotDir));
            }
            tarSnapshotFiles.add(outputFile);
        }
        return tarSnapshotFiles;
    }

    private void deleteSnapshotDir(Set<String> snapshotDirs,
                                   Closure done) {
        // Delete all backend snapshot dir
        for (String snapshotDir : snapshotDirs) {
            try {
                FileUtils.deleteDirectory(new File(snapshotDir));
            } catch (IOException e) {
                done.run(new Status(RaftError.EIO,
                                    "Failed to delete backend snapshot dir " +
                                    snapshotDir));
            }
        }
    }

    private String writeManifest(SnapshotWriter writer,
                                 Set<String> snapshotFiles,
                                 Closure done) {
        String writerPath = writer.getPath();
        // Write all backend compressed snapshot file path to manifest
        String jraftSnapshotPath = Paths.get(writerPath, SNAPSHOT_DIR)
                                        .toString();
        File snapshotManifest = new File(jraftSnapshotPath, MANIFEST);
        try {
            FileUtils.writeLines(snapshotManifest, snapshotFiles);
        } catch (IOException e) {
            done.run(new Status(RaftError.EIO,
                                "Failed to write backend snapshot file path " +
                                "to manifest"));
        }
        return jraftSnapshotPath;
    }

    private void compressJraftSnapshotDir(SnapshotWriter writer,
                                          String jraftSnapshotPath,
                                          Closure done) {
        String writerPath = writer.getPath();
        String outputFile = Paths.get(writerPath, SNAPSHOT_ARCHIVE).toString();
        try {
            LocalFileMeta.Builder metaBuilder = LocalFileMeta.newBuilder();
            Checksum checksum = new CRC64();
            CompressUtil.compressTar(jraftSnapshotPath, outputFile, checksum);
            metaBuilder.setChecksum(Long.toHexString(checksum.getValue()));
            if (writer.addFile(SNAPSHOT_ARCHIVE, metaBuilder.build())) {
                done.run(Status.OK());
            } else {
                done.run(new Status(RaftError.EIO,
                                    "Failed to add snapshot file: %s",
                                    writerPath));
            }
        } catch (final Throwable t) {
            LOG.error("Failed to compress snapshot, path={}, files={}, {}.",
                      writerPath, writer.listFiles(), t);
            done.run(new Status(RaftError.EIO,
                                "Failed to compress snapshot at %s, error is %s",
                                writerPath, t.getMessage()));
        }
    }

    private void decompressSnapshot(String readerPath, LocalFileMeta meta)
                                    throws IOException {
        String archiveFile = Paths.get(readerPath, SNAPSHOT_ARCHIVE).toString();
        Checksum checksum = new CRC64();
        CompressUtil.decompressTar(archiveFile, readerPath, checksum);
        if (meta.hasChecksum()) {
            E.checkArgument(meta.getChecksum().equals(
                            Long.toHexString(checksum.getValue())),
                            "Snapshot checksum failed");
        }
    }
}
