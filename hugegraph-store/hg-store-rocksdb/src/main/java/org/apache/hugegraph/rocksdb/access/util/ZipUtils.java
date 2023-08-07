/*
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

package org.apache.hugegraph.rocksdb.access.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;
import java.util.zip.Checksum;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class ZipUtils {
    // /**
    //  * create a direcotry if not exist
    //  * 
    //  * @param dir
    //  */
    // public static void createDirectory(String dir) throws IOException {
    //     Path path = Paths.get(dir);
    //     if (!Files.exists(path)) {
    //         Files.createDirectories(path);
    //     }
    // }

    // /**
    //  * delete the directory recursively with all its files and subdirectories
    //  * 
    //  * @param dir
    //  */
    // public static void deleteDirectory(String dir) throws IOException {
    //     Path path = Paths.get(dir);
    //     if (Files.exists(path)) {
    //         Files.walk(path).sorted(Comparator.reverseOrder()).map(Path::toFile)
    //                 .forEach(File::delete);
    //     }
    // }

    // /**
    //  * move a directory form source path to target path target will be deleted if exist
    //  * 
    //  * @param source
    //  * @param target
    //  * @throws IOException
    //  */
    // public static void moveDirectory(String source, String target) throws IOException {
    //     ZipUtils.deleteDirectory(target);
    //     Files.move(Paths.get(source), Paths.get(target), StandardCopyOption.REPLACE_EXISTING);
    // }

    public static void compress(final String rootDir, final String sourceDir,
                                final String outputFile, final Checksum checksum) throws
                                                                                  IOException {
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
