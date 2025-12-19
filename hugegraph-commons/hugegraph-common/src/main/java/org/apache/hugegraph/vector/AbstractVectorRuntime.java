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

package org.apache.hugegraph.vector;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hugegraph.util.JsonUtilCommon;

import io.github.jbellis.jvector.graph.GraphIndex;
import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;

public abstract class AbstractVectorRuntime<Id> implements VectorIndexRuntime<Id> {

    public String basePath = "";
    protected static final String CURRENT_VERSION_LINK_NAME = "current";
    protected static final String TEMP_LINK_NAME = "current_temp";
    private static final String VERSION_PREFIX = "version_";
    protected static final String INDEX_FILE_NAME = "index.inline";
    protected static final String META_FILE_NAME = "vector_meta.json";

    protected final ConcurrentMap<Id, IndexContext<Id>> vectorMap = new ConcurrentHashMap<>();

    public AbstractVectorRuntime(String basePath) {
        basePath = basePath;
    }

    public static class IndexContext<Id> {

        final Id indexLabelId;

        final RandomAccessVectorValues vectors; // per-index RAVV
        final GraphIndexBuilder builder;        // owns the mutable OnHeapGraphIndex
        VectorSimilarityFunction similarityFunction;
        int dimension;
        IndexContextMetaData metaData;

        IndexContext(Id indexLabelId,
                     RandomAccessVectorValues vectors,
                     GraphIndexBuilder builder,
                     long watermark,
                     int dimension,
                     VectorSimilarityFunction similarityFunction) {
            this.indexLabelId = indexLabelId;
            this.vectors = vectors;
            this.builder = builder;
            this.similarityFunction = similarityFunction;
            this.dimension = dimension;
            this.metaData = new IndexContextMetaData(0, watermark, null);
        }

        GraphIndex graphView() {
            return builder.getGraph();
        }

        IndexContextMetaData metaData() { return metaData; }

        void setMetaData(IndexContextMetaData metaData) { this.metaData = metaData; }

        public static class IndexContextMetaData {
            private final int nextOrd;
            private final long watermark;
            private final List<Integer> freeOrd;

            IndexContextMetaData(int nextOrd, long watermark, List<Integer> freeOrd) {
                this.nextOrd = nextOrd;
                this.watermark = watermark;
                this.freeOrd = freeOrd;
            }

            int getNextOrd() { return nextOrd; }

            long getWatermark() { return watermark; }

            List<Integer> getFreeOrd() { return freeOrd; }
        }
    }

    @Override
    public void init() {

    }

    @Override
    public void stop() {
        //    TODO: 1. flush all the context to disk
    }

    @Override
    public void flush(Id indexlabelId) throws IOException {
        IndexContext<Id> context = obtainContext(indexlabelId);
        String contextMetaDataJsonString = JsonUtilCommon.toJson(context.metaData());
        String pathString = basePath + "/" + (indexlabelId) + "/";
        Path indexBaseDir = Paths.get(pathString);

        // create temp path
        String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
        Path newVersionDir = indexBaseDir.resolve(VERSION_PREFIX + "_" + timestamp);
        Files.createDirectories(newVersionDir);

        try {
            Path indexPath = newVersionDir.resolve(INDEX_FILE_NAME);
            Files.createFile(indexPath);
            context.builder.cleanup();
            OnDiskGraphIndex.write(context.builder.getGraph(), context.vectors, indexPath);

            Path metaPath = newVersionDir.resolve(META_FILE_NAME);
            Files.write(metaPath, JsonUtilCommon.toJson(context.metaData()).getBytes());

            // Sync to filesystem
            forceSyncDirectory(newVersionDir);

            Path tempSymlink = indexBaseDir.resolve(TEMP_LINK_NAME);
            Files.deleteIfExists(tempSymlink);
            Files.createSymbolicLink(tempSymlink, newVersionDir);

            Path currentSymlink = getOnDiskIndexDirPath(indexlabelId);
            // Atomically rename the temporary link to 'current'. THIS IS THE SWITCH.
            Files.move(tempSymlink, currentSymlink,
                       StandardCopyOption.ATOMIC_MOVE,
                       StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            System.err.println("Atomic save failed: " + e.getMessage());
            throw e;
        }
    }

    private void forceSyncDirectory(Path directory) throws IOException {
        // need test to prove if it is work

        // First, fsync all files within the directory
        if (!Files.isDirectory(directory)) {
            throw new IllegalArgumentException("Path is not a directory: " + directory);
        }

        try (Stream<Path> stream = Files.list(directory)) {
            for (Path file : stream.collect(Collectors.toList())) {
                if (!Files.isRegularFile(file)) { continue; }
                try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
                    channel.force(true);
                }
            }
        }
        // Then, fsync the directory itself to persist its metadata (the file entries)
        try (RandomAccessFile raf = new RandomAccessFile(directory.toFile(), "r");
             FileChannel ch = raf.getChannel()) {
            ch.force(true);
        }
    }

    @Override
    public long getCurrentSequence(Id indexlabelId) {
        if (!this.vectorMap.containsKey(indexlabelId)) {
            return -1;
        }
        return vectorMap.get(indexlabelId).metaData().getWatermark();
    }

    @Override
    public int getNextVectorId(Id indexlabelId) {
        if (!this.vectorMap.containsKey(indexlabelId)) {
            return -1;
        }
        return vectorMap.get(indexlabelId).metaData().getNextOrd();
    }

    protected final IndexContext<Id> obtainContext(Id indexlabelId) {
        // TODO:add the function that update the ord and sequence in the context
        IndexContext<Id> context = getContext(indexlabelId);
        if (context != null) {
            return context;
        }
        return createNewContext(indexlabelId);
    }

    protected abstract IndexContext<Id> createNewContext(Id indexlabelId);

    IndexContext<Id> getContext(Id indexlabelId) {
        if (this.vectorMap.containsKey(indexlabelId)) {
            return vectorMap.get(indexlabelId);
        }
        return null;
    }

    boolean checkPathValid(Id indexlabelId) {
        String pathString = basePath + "/" + (indexlabelId) + "/";
        Path indexBaseDir = Paths.get(pathString);
        if (!Files.isDirectory(indexBaseDir)) {
            System.err.println(
                    "Validation failed: Base directory does not exist or is not a directory: " +
                    indexBaseDir);
            return false;
        }
        // 2. check the current link path
        Path currentLinkPath = indexBaseDir.resolve(CURRENT_VERSION_LINK_NAME);
        if (!Files.isSymbolicLink(currentLinkPath)) {
            System.err.println(
                    "Validation failed: 'current' is not a symbolic link or does not exist in: " +
                    indexBaseDir);
            return false;
        }

        try {
            // 3. read the real Path dir
            Path realVersionDir = Files.readSymbolicLink(currentLinkPath);
            // transform to the absolute path
            if (!realVersionDir.isAbsolute()) {
                realVersionDir = indexBaseDir.resolve(realVersionDir).toAbsolutePath();
            }
            // 4. check the dir existed
            if (!Files.isDirectory(realVersionDir)) {
                System.err.println(
                        "Validation failed: 'current' points to a non-existent directory: " +
                        realVersionDir);
                return false;
            }
            // 5. check 2 files of index existed
            Path indexFilePath = realVersionDir.resolve(INDEX_FILE_NAME);
            Path metaFilePath = realVersionDir.resolve(META_FILE_NAME);

            boolean indexFileExists = Files.isRegularFile(indexFilePath);
            boolean metaFileExists = Files.isRegularFile(metaFilePath);
            if (!indexFileExists) {
                System.err.println(
                        "Validation failed: Index file not found in version directory: " +
                        indexFilePath);
            }
            if (!metaFileExists) {
                System.err.println(
                        "Validation failed: Metadata file not found in version directory: " +
                        metaFilePath);
            }

            return indexFileExists && metaFileExists;

        } catch (IOException e) {
            System.err.println(
                    "An I/O error occurred during validation for index " + indexlabelId + ": " +
                    e.getMessage());
            return false;
        }
    }

    Path getOnDiskIndexDirPath(Id indexlabelId) throws IOException {
        String pathString = basePath + "/" + (indexlabelId) + "/";
        Path indexBaseDir = Paths.get(pathString);
        Path currentLinkPath = indexBaseDir.resolve(CURRENT_VERSION_LINK_NAME);
        return Files.readSymbolicLink(currentLinkPath);
    }

    protected abstract String idToString(Id id);
}
