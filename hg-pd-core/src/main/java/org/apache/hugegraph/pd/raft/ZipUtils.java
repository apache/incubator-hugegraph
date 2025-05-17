package org.apache.hugegraph.pd.raft;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;

import java.io.*;
import java.nio.file.Paths;
import java.util.zip.*;

@Slf4j
public final class ZipUtils {

    public static void compress(final String rootDir, final String sourceDir,
            final String outputFile, final Checksum checksum) throws IOException {
        try (final FileOutputStream fos = new FileOutputStream(outputFile);
                final CheckedOutputStream cos = new CheckedOutputStream(fos, checksum);
                final ZipOutputStream zos = new ZipOutputStream(new BufferedOutputStream(cos))) {
            ZipUtils.compressDirectoryToZipFile(rootDir, sourceDir, zos);
            zos.flush();
            fos.getFD().sync();
        }
    }

    private static void compressDirectoryToZipFile(final String rootDir, final String sourceDir,
            final ZipOutputStream zos) throws IOException {
        final String dir = Paths.get(rootDir, sourceDir).toString();
        final File[] files = new File(dir).listFiles();
        for (final File file : files) {
            final String child = Paths.get(sourceDir, file.getName()).toString();
            if (file.isDirectory()) {
                compressDirectoryToZipFile(rootDir, child, zos);
            } else {
                zos.putNextEntry(new ZipEntry(child));
                try (final FileInputStream fis = new FileInputStream(file);
                        final BufferedInputStream bis = new BufferedInputStream(fis)) {
                    IOUtils.copy(bis, zos);
                }
            }
        }
    }

    public static void decompress(final String sourceFile, final String outputDir,
            final Checksum checksum) throws IOException {
        try (final FileInputStream fis = new FileInputStream(sourceFile);
                final CheckedInputStream cis = new CheckedInputStream(fis, checksum);
                final ZipInputStream zis = new ZipInputStream(new BufferedInputStream(cis))) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                final String fileName = entry.getName();
                final File entryFile = new File(Paths.get(outputDir, fileName).toString());
                FileUtils.forceMkdir(entryFile.getParentFile());
                try (final FileOutputStream fos = new FileOutputStream(entryFile);
                        final BufferedOutputStream bos = new BufferedOutputStream(fos)) {
                    IOUtils.copy(zis, bos);
                    bos.flush();
                    fos.getFD().sync();
                }
            }
            IOUtils.copy(cis, NullOutputStream.NULL_OUTPUT_STREAM);
        }
    }
}
