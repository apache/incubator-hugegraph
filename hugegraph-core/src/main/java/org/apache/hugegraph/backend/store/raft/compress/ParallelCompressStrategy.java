package org.apache.hugegraph.backend.store.raft.compress;

import com.alipay.sofa.jraft.util.ExecutorServiceHelper;
import com.alipay.sofa.jraft.util.NamedThreadFactory;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.ThreadPoolUtil;
import com.google.common.collect.Lists;
import org.apache.commons.compress.archivers.zip.*;
import org.apache.commons.compress.parallel.InputStreamSupplier;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.NullInputStream;
import org.apache.hugegraph.config.CoreOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.*;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;
import java.util.zip.Checksum;
import java.util.zip.ZipEntry;

public class ParallelCompressStrategy implements CompressStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(ParallelCompressStrategy.class);

    public static final int QUEUE_SIZE = CoreOptions.CPUS;
    public static final long KEEP_ALIVE_SECOND = 300L;

    private final int compressThreads;
    private final int decompressThreads;

    public ParallelCompressStrategy(final int compressThreads, final int decompressThreads) {
        this.compressThreads = compressThreads;
        this.decompressThreads = decompressThreads;
    }

    /**
     * Parallel output streams controller
     */
    private static class ZipArchiveScatterOutputStream {

        private final ParallelScatterZipCreator creator;

        public ZipArchiveScatterOutputStream(final ExecutorService executorService) {
            this.creator = new ParallelScatterZipCreator(executorService);
        }

        public void addEntry(final ZipArchiveEntry entry, final InputStreamSupplier supplier) {
            creator.addArchiveEntry(entry, supplier);
        }

        public void writeTo(final ZipArchiveOutputStream archiveOutput) throws Exception {
            creator.writeTo(archiveOutput);
        }

    }

    @Override
    public void compressZip(final String rootDir, final String sourceDir, final String outputZipFile,
                            final Checksum checksum) throws Throwable {
        final File rootFile = new File(Paths.get(rootDir, sourceDir).toString());
        final File zipFile = new File(outputZipFile);
        LOG.info("Start to compress snapshot in parallel mode");
        FileUtils.forceMkdir(zipFile.getParentFile());

        final ExecutorService compressExecutor = newFixedPool(compressThreads, compressThreads,
            "raft-snapshot-compress-executor", new ThreadPoolExecutor.CallerRunsPolicy());
        final ZipArchiveScatterOutputStream scatterOutput = new ZipArchiveScatterOutputStream(compressExecutor);
        compressDirectoryToZipFile(rootFile, scatterOutput, sourceDir, ZipEntry.DEFLATED);

        try (final FileOutputStream fos = new FileOutputStream(zipFile);
             final BufferedOutputStream bos = new BufferedOutputStream(fos);
             final CheckedOutputStream cos = new CheckedOutputStream(bos, checksum);
             final ZipArchiveOutputStream archiveOutputStream = new ZipArchiveOutputStream(cos)) {
            scatterOutput.writeTo(archiveOutputStream);
            archiveOutputStream.flush();
            fos.getFD().sync();
        }

        ExecutorServiceHelper.shutdownAndAwaitTermination(compressExecutor);
    }

    @Override
    public void decompressZip(final String sourceZipFile, final String outputDir, final Checksum checksum)
        throws Throwable {
        LOG.info("Start to decompress snapshot in parallel mode");
        final ExecutorService decompressExecutor = newFixedPool(decompressThreads, decompressThreads,
            "raft-snapshot-decompress-executor", new ThreadPoolExecutor.CallerRunsPolicy());
        // compute the checksum in a single thread
        final Future<Boolean> checksumFuture = decompressExecutor.submit(() -> {
            computeZipFileChecksumValue(sourceZipFile, checksum);
            return true;
        });

        try (final ZipFile zipFile = new ZipFile(sourceZipFile)) {
            final List<Future<Boolean>> futures = Lists.newArrayList();
            for (final Enumeration<ZipArchiveEntry> e = zipFile.getEntries(); e.hasMoreElements(); ) {
                final ZipArchiveEntry zipEntry = e.nextElement();
                final Future<Boolean> future = decompressExecutor.submit(() -> {
                    unZipFile(zipFile, zipEntry, outputDir);
                    return true;
                });
                futures.add(future);
            }
            // blocking and caching exception
            for (final Future<Boolean> future : futures) {
                future.get();
            }
        }
        // wait for checksum to be calculated
        checksumFuture.get();
        ExecutorServiceHelper.shutdownAndAwaitTermination(decompressExecutor);
    }

    private void compressDirectoryToZipFile(final File dir, final ZipArchiveScatterOutputStream scatterOutput,
                                            final String sourceDir, final int method) {
        if (dir == null) {
            return;
        }
        if (dir.isFile()) {
            addEntry(sourceDir, dir, scatterOutput, method);
            return;
        }
        final File[] files = Requires.requireNonNull(dir.listFiles(), "files");
        for (final File file : files) {
            final String child = Paths.get(sourceDir, file.getName()).toString();
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
    private void addEntry(final String filePath, final File file, final ZipArchiveScatterOutputStream scatterOutputStream,
                          final int method) {
        final ZipArchiveEntry archiveEntry = new ZipArchiveEntry(filePath);
        archiveEntry.setMethod(method);
        scatterOutputStream.addEntry(archiveEntry, () -> {
            try {
                return file.isDirectory() ? new NullInputStream(0) :
                    new BufferedInputStream(new FileInputStream(file));
            } catch (final FileNotFoundException e) {
                LOG.error("Can't find file, path={}, {}", file.getPath(), e);
            }
            return new NullInputStream(0);
        });
    }

    /**
     * Unzip the archive entry to targetDir
     */
    private void unZipFile(final ZipFile zipFile, final ZipArchiveEntry entry, final String targetDir)
        throws Exception {
        final File targetFile = new File(Paths.get(targetDir, entry.getName()).toString());
        FileUtils.forceMkdir(targetFile.getParentFile());
        try (final InputStream is = zipFile.getInputStream(entry);
             final BufferedInputStream fis = new BufferedInputStream(is);
             final BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(targetFile))) {
            IOUtils.copy(fis, bos);
        }
    }

    /**
     * Compute the value of checksum
     */
    private void computeZipFileChecksumValue(final String zipPath, final Checksum checksum) throws Exception {
        try (final BufferedInputStream bis = new BufferedInputStream(new FileInputStream(zipPath));
             final CheckedInputStream cis = new CheckedInputStream(bis, checksum);
             final ZipArchiveInputStream zis = new ZipArchiveInputStream(cis)) {
            // checksum is calculated in the process
            while ((zis.getNextZipEntry()) != null)
                ;
        }
    }

    private static ExecutorService newFixedPool(int coreThreads, int maxThreads,
                                           String name,
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
