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
import java.util.concurrent.CompletableFuture;
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
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.baidu.hugegraph.util.ZipUtil;

public class StoreSnapshotFile {

    private static final Logger LOG = Log.logger(StoreSnapshotFile.class);

    private static final String SNAPSHOT_DIR = "ss";
    private static final String SNAPSHOT_ARCHIVE = "ss.zip";

    public void save(BackendStore store, SnapshotWriter writer,
                     Closure done, ExecutorService executor) {
        String writerPath = writer.getPath();
        String snapshotPath = Paths.get(writerPath, SNAPSHOT_DIR).toString();
        try {
            this.doSnapshotSave(store, snapshotPath)
                .whenComplete((metaBuilder, throwable) -> {
                if (throwable == null) {
                    executor.execute(() -> compressSnapshot(writer, metaBuilder,
                                                            done));
                } else {
                    LOG.error("Failed to save snapshot, path={}, files={}, {}.",
                              writerPath, writer.listFiles(), throwable);
                    done.run(new Status(RaftError.EIO,
                             "Failed to save snapshot at %s, error is %s",
                             writerPath, throwable.getMessage()));
                }
            });
        } catch (Throwable t) {
            LOG.error("Failed to save snapshot, path={}, files={}, {}.",
                      writerPath, writer.listFiles(), t);
            done.run(new Status(RaftError.EIO,
                                "Failed to save snapshot at %s, error is %s",
                                writerPath, t.getMessage()));
        }
    }

    public boolean load(BackendStore store, SnapshotReader reader) {
        LocalFileMeta meta = (LocalFileMeta) reader.getFileMeta(SNAPSHOT_ARCHIVE);
        String readerPath = reader.getPath();
        if (meta == null) {
            LOG.error("Can't find snapshot file, path={}.", readerPath);
            return false;
        }
        String snapshotPath = Paths.get(readerPath, SNAPSHOT_DIR).toString();
        try {
            this.decompressSnapshot(readerPath, meta);
            this.doSnapshotLoad(store, snapshotPath);
            File tmp = new File(snapshotPath);
            // Delete the decompressed temporary file. If the deletion fails
            // (although it is a small probability event), it may affect the
            // next snapshot decompression result. Therefore, the safest way
            // is to terminate the state machine immediately. Users can choose
            // to manually delete and restart according to the log information.
            if (tmp.exists()) {
                FileUtils.forceDelete(new File(snapshotPath));
            }
            return true;
        } catch (Throwable t) {
            LOG.error("Failed to load snapshot, path={}, file list={}, {}.",
                      readerPath, reader.listFiles(), t);
            return false;
        }
    }

    public CompletableFuture<LocalFileMeta.Builder> doSnapshotSave(
                                                    BackendStore store,
                                                    String snapshotPath) {
        store.writeSnapshot(snapshotPath);
        return CompletableFuture.completedFuture(LocalFileMeta.newBuilder());
    }

    public void doSnapshotLoad(BackendStore store, String snapshotPath) {
        store.readSnapshot(snapshotPath);
    }

    private void compressSnapshot(SnapshotWriter writer,
                                  LocalFileMeta.Builder metaBuilder,
                                  Closure done) {
        String writerPath = writer.getPath();
        String outputFile = Paths.get(writerPath, SNAPSHOT_ARCHIVE).toString();
        try {
            Checksum checksum = new CRC64();
            ZipUtil.compress(writerPath, SNAPSHOT_DIR, outputFile, checksum);
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
        String sourceFile = Paths.get(readerPath, SNAPSHOT_ARCHIVE).toString();
        Checksum checksum = new CRC64();
        ZipUtil.decompress(sourceFile, readerPath, checksum);
        if (meta.hasChecksum()) {
            E.checkArgument(meta.getChecksum().equals(
                            Long.toHexString(checksum.getValue())),
                            "Snapshot checksum failed");
        }
    }
}
