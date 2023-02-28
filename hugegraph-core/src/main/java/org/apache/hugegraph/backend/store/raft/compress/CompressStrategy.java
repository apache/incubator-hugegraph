package org.apache.hugegraph.backend.store.raft.compress;

import java.util.zip.Checksum;

public interface CompressStrategy {

    void compressZip(final String rootDir, final String sourceDir, final String outputZipFile, final Checksum checksum)
        throws Throwable;

    void decompressZip(final String sourceZipFile, final String outputDir, final Checksum checksum) throws Throwable;
}
