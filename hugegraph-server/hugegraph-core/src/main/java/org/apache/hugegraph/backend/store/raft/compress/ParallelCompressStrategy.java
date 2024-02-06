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

package org.apache.hugegraph.backend.store.raft.compress;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;
import java.util.zip.Checksum;
import java.util.zip.ZipEntry;

import org.apache.commons.compress.archivers.zip.ParallelScatterZipCreator;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.compress.parallel.InputStreamSupplier;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.NullInputStream;
import org.apache.hugegraph.config.CoreOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.util.ExecutorServiceHelper;
import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.ThreadPoolUtil;
import com.google.common.collect.Lists;

public class ParallelCompressStrategy implements CompressStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelCompressStrategy.class);

    public static final int QUEUE_SIZE = CoreOptions.CPUS;
    public static final long KEEP_ALIVE_SECOND = 300L;

    private final int compressThreads;
    private final int decompressThreads;

    public ParallelCompressStrategy(int compressThreads, int decompressThreads) {
        this.compressThreads = compressThreads;
        this.decompressThreads = decompressThreads;
    }

    /**
     * Parallel output streams controller
     */
    private static class ZipArchiveScatterOutputStream {

        private final ParallelScatterZipCreator creator;

        public ZipArchiveScatterOutputStream(ExecutorService executorService) {
            this.creator = new ParallelScatterZipCreator(executorService);
        }

        public void addEntry(ZipArchiveEntry entry, InputStreamSupplier supplier) {
            creator.addArchiveEntry(entry, supplier);
        }

        public void writeTo(ZipArchiveOutputStream archiveOutput) throws Exception {
            creator.writeTo(archiveOutput);
        }
    }

    @Override
    public void compressZip(String rootDir, String sourceDir, String outputZipFile,
                            Checksum checksum) throws Throwable {
        File rootFile = new File(Paths.get(rootDir, sourceDir).toString());
        File zipFile = new File(outputZipFile);
        LOG.info("Start to compress snapshot in parallel mode");
        FileUtils.forceMkdir(zipFile.getParentFile());

        ExecutorService compressExecutor =
            newFixedPool(compressThreads, compressThreads, "raft-snapshot-compress-executor",
                         new ThreadPoolExecutor.CallerRunsPolicy());
        ZipArchiveScatterOutputStream scatterOutput =
            new ZipArchiveScatterOutputStream(compressExecutor);
        compressDirectoryToZipFile(rootFile, scatterOutput, sourceDir, ZipEntry.DEFLATED);

        try (FileOutputStream fos = new FileOutputStream(zipFile);
             BufferedOutputStream bos = new BufferedOutputStream(fos);
             CheckedOutputStream cos = new CheckedOutputStream(bos, checksum);
             ZipArchiveOutputStream archiveOutputStream = new ZipArchiveOutputStream(cos)) {
            scatterOutput.writeTo(archiveOutputStream);
            archiveOutputStream.flush();
            fos.getFD().sync();
        }

        ExecutorServiceHelper.shutdownAndAwaitTermination(compressExecutor);
    }

    @Override
    public void decompressZip(String sourceZipFile, String outputDir,
                              Checksum checksum) throws Throwable {
        LOG.info("Start to decompress snapshot in parallel mode");
        ExecutorService decompressExecutor =
            newFixedPool(decompressThreads, decompressThreads,
                         "raft-snapshot-decompress-executor",
                         new ThreadPoolExecutor.CallerRunsPolicy());
        // compute the checksum in a single thread
        Future<Boolean> checksumFuture = decompressExecutor.submit(() -> {
            computeZipFileChecksumValue(sourceZipFile, checksum);
            return true;
        });

        try (ZipFile zipFile = new ZipFile(sourceZipFile)) {
            List<Future<Boolean>> futures = Lists.newArrayList();
            for (Enumeration<ZipArchiveEntry> e = zipFile.getEntries();
                 e.hasMoreElements(); ) {
                ZipArchiveEntry zipEntry = e.nextElement();
                Future<Boolean> future = decompressExecutor.submit(() -> {
                    unZipFile(zipFile, zipEntry, outputDir);
                    return true;
                });
                futures.add(future);
            }
            // blocking and caching exception
            for (Future<Boolean> future : futures) {
                future.get();
            }
        }
        // wait for checksum to be calculated
        checksumFuture.get();
        ExecutorServiceHelper.shutdownAndAwaitTermination(decompressExecutor);
    }

    private void compressDirectoryToZipFile(File dir, ZipArchiveScatterOutputStream scatterOutput,
                                            String sourceDir, int method) {
        if (dir == null) {
            return;
        }
        if (dir.isFile()) {
            addEntry(sourceDir, dir, scatterOutput, method);
            return;
        }
        File[] files = Requires.requireNonNull(Objects.requireNonNull(dir.listFiles()), "files");
        for (File file : files) {
            String child = Paths.get(sourceDir, file.getName()).toString();
            if (file.isDirectory()) {
                compressDirectoryToZipFile(file, scatterOutput, child, method);
            } else {
                addEntry(child, file, scatterOutput, method);
            }
        }
    }

    /**
     * Add archive entry to the scatterOutputStream
     */
    private void addEntry(String filePath, File file,
                          ZipArchiveScatterOutputStream scatterOutputStream, int method) {
        ZipArchiveEntry archiveEntry = new ZipArchiveEntry(filePath);
        archiveEntry.setMethod(method);
        scatterOutputStream.addEntry(archiveEntry, () -> {
            try {
                return file.isDirectory() ? new NullInputStream(0)
                                          : new BufferedInputStream(new FileInputStream(file));
            } catch (FileNotFoundException e) {
                LOG.error("Can't find file, path={}, {}", file.getPath(), e);
            }
            return new NullInputStream(0);
        });
    }

    /**
     * Unzip the archive entry to targetDir
     */
    private void unZipFile(ZipFile zipFile, ZipArchiveEntry entry,
                           String targetDir) throws Exception {
        File targetFile = new File(Paths.get(targetDir, entry.getName()).toString());
        if (!targetFile.toPath().normalize().startsWith(targetDir)) {
            throw new IOException(String.format("Bad entry: %s", entry.getName()));
        }

        FileUtils.forceMkdir(targetFile.getParentFile());
        try (InputStream is = zipFile.getInputStream(entry);
             BufferedInputStream fis = new BufferedInputStream(is);
             BufferedOutputStream bos =
                 new BufferedOutputStream(Files.newOutputStream(targetFile.toPath()))) {
            IOUtils.copy(fis, bos);
        }
    }

    /**
     * Compute the value of checksum
     */
    private void computeZipFileChecksumValue(String zipPath, Checksum checksum) throws Exception {
        try (BufferedInputStream bis =
                 new BufferedInputStream(Files.newInputStream(Paths.get(zipPath)));
             CheckedInputStream cis = new CheckedInputStream(bis, checksum);
             ZipArchiveInputStream zis = new ZipArchiveInputStream(cis)) {
            // checksum is calculated in the process
            while (zis.getNextZipEntry() != null) {
                // TODO: any better way to do the check?
            }
        }
    }

    private static ExecutorService newFixedPool(int coreThreads, int maxThreads, String name,
                                                RejectedExecutionHandler handler) {
        BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(QUEUE_SIZE);
        return ThreadPoolUtil.newBuilder()
                             .poolName(name)
                             .enableMetric(false)
                             .coreThreads(coreThreads)
                             .maximumThreads(maxThreads)
                             .keepAliveSeconds(KEEP_ALIVE_SECOND)
                             .workQueue(queue)
                             .threadFactory(new NamedThreadFactory(name, true))
                             .rejectedHandler(handler)
                             .build();
    }
}
