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

package com.baidu.hugegraph.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;
import java.util.zip.Checksum;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;

import com.baidu.hugegraph.backend.store.raft.RaftSharedContext;

import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

public final class CompressUtil {

    /**
     * Reference: https://mkyong.com/java/how-to-create-tar-gz-in-java/
     */
    public static void compressTar(String inputDir, String outputFile,
                                   Checksum checksum) throws IOException {
        LZ4Factory factory = LZ4Factory.fastestInstance();
        LZ4Compressor compressor = factory.fastCompressor();
        int blockSize = RaftSharedContext.BLOCK_SIZE;
        try (FileOutputStream fos = new FileOutputStream(outputFile);
             CheckedOutputStream cos = new CheckedOutputStream(fos, checksum);
             BufferedOutputStream bos = new BufferedOutputStream(cos);
             LZ4BlockOutputStream lz4os = new LZ4BlockOutputStream(bos,
                                                                   blockSize,
                                                                   compressor);
             TarArchiveOutputStream tos = new TarArchiveOutputStream(lz4os)) {
            Path source = Paths.get(inputDir);
            CompressUtil.tarDir(source, tos);
            tos.flush();
            fos.getFD().sync();
        }
    }

    private static void tarDir(Path source, TarArchiveOutputStream tos)
                               throws IOException {
        Files.walkFileTree(source, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir,
                                                     BasicFileAttributes attrs)
                                                     throws IOException {
                String entryName = buildTarEntryName(source, dir);
                if (!entryName.isEmpty()) {
                    TarArchiveEntry entry = new TarArchiveEntry(dir.toFile(),
                                                                entryName);
                    tos.putArchiveEntry(entry);
                    tos.closeArchiveEntry();
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file,
                                             BasicFileAttributes attributes)
                                             throws IOException {
                // Only copy files, no symbolic links
                if (attributes.isSymbolicLink()) {
                    return FileVisitResult.CONTINUE;
                }
                String targetFile = buildTarEntryName(source, file);
                TarArchiveEntry entry = new TarArchiveEntry(file.toFile(),
                                                            targetFile);
                tos.putArchiveEntry(entry);
                Files.copy(file, tos);
                tos.closeArchiveEntry();
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException e) {
                return FileVisitResult.TERMINATE;
            }
        });
    }

    private static String buildTarEntryName(Path topLevel, Path current) {
        return topLevel.getFileName().resolve(topLevel.relativize(current))
                       .toString();
    }

    public static void decompressTar(String sourceFile, String outputDir,
                                     Checksum checksum) throws IOException {
        Path source = Paths.get(sourceFile);
        Path target = Paths.get(outputDir);
        if (Files.notExists(source)) {
            throw new IOException(String.format(
                                  "The source file %s doesn't exists", source));
        }
        LZ4Factory factory = LZ4Factory.fastestInstance();
        LZ4FastDecompressor decompressor = factory.fastDecompressor();
        try (InputStream fis = Files.newInputStream(source);
             CheckedInputStream cis = new CheckedInputStream(fis, checksum);
             BufferedInputStream bis = new BufferedInputStream(cis);
             LZ4BlockInputStream lz4is = new LZ4BlockInputStream(bis,
                                                                 decompressor);
             TarArchiveInputStream tis = new TarArchiveInputStream(lz4is)) {
            ArchiveEntry entry;
            while ((entry = tis.getNextEntry()) != null) {
                // Create a new path, zip slip validate
                Path newPath = zipSlipProtect(entry, target);
                if (entry.isDirectory()) {
                    Files.createDirectories(newPath);
                } else {
                    // check parent folder again
                    Path parent = newPath.getParent();
                    if (parent != null) {
                        if (Files.notExists(parent)) {
                            Files.createDirectories(parent);
                        }
                    }
                    // Copy TarArchiveInputStream to Path newPath
                    Files.copy(tis, newPath,
                               StandardCopyOption.REPLACE_EXISTING);
                }
            }
        }
    }

    private static Path zipSlipProtect(ArchiveEntry entry, Path targetDir)
                                       throws IOException {
        Path targetDirResolved = targetDir.resolve(entry.getName());
        /*
         * Make sure normalized file still has targetDir as its prefix,
         * else throws exception
         */
        Path normalizePath = targetDirResolved.normalize();
        if (!normalizePath.startsWith(targetDir)) {
            throw new IOException(String.format("Bad entry: %s",
                                                entry.getName()));
        }
        return normalizePath;
    }

    public static void compressZip(String rootDir, String sourceDir,
                                   String outputFile, Checksum checksum)
                                   throws IOException {
        try (FileOutputStream fos = new FileOutputStream(outputFile);
             CheckedOutputStream cos = new CheckedOutputStream(fos, checksum);
             BufferedOutputStream bos = new BufferedOutputStream(cos);
             ZipOutputStream zos = new ZipOutputStream(bos)) {
            CompressUtil.zipDir(rootDir, sourceDir, zos);
            zos.flush();
            fos.getFD().sync();
        }
    }

    private static void zipDir(String rootDir, String sourceDir,
                               ZipOutputStream zos) throws IOException {
        String dir = Paths.get(rootDir, sourceDir).toString();
        File[] files = new File(dir).listFiles();
        E.checkNotNull(files, "files");
        for (File file : files) {
            String child = Paths.get(sourceDir, file.getName()).toString();
            if (file.isDirectory()) {
                zipDir(rootDir, child, zos);
            } else {
                zos.putNextEntry(new ZipEntry(child));
                try (FileInputStream fis = new FileInputStream(file);
                     BufferedInputStream bis = new BufferedInputStream(fis)) {
                    IOUtils.copy(bis, zos);
                }
            }
        }
    }

    public static void decompressZip(String sourceFile, String outputDir,
                                     Checksum checksum) throws IOException {
        try (FileInputStream fis = new FileInputStream(sourceFile);
             CheckedInputStream cis = new CheckedInputStream(fis, checksum);
             BufferedInputStream bis = new BufferedInputStream(cis);
             ZipInputStream zis = new ZipInputStream(bis)) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                String fileName = entry.getName();
                File entryFile = new File(Paths.get(outputDir, fileName)
                                               .toString());
                FileUtils.forceMkdir(entryFile.getParentFile());
                try (FileOutputStream fos = new FileOutputStream(entryFile);
                     BufferedOutputStream bos = new BufferedOutputStream(fos)) {
                    IOUtils.copy(zis, bos);
                    bos.flush();
                    fos.getFD().sync();
                }
            }
            /*
             * Continue to read all remaining bytes(extra metadata of ZipEntry)
             * directly from the checked stream, Otherwise, the checksum value
             * maybe unexpected.
             * See https://coderanch.com/t/279175/java/ZipInputStream
             */
            IOUtils.copy(cis, NullOutputStream.NULL_OUTPUT_STREAM);
        }
    }
}
