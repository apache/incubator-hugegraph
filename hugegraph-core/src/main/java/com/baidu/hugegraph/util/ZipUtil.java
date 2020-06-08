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

package com.baidu.hugegraph.util;

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

public final class ZipUtil {

    public static void compress(String rootDir, String sourceDir,
                                String outputFile, Checksum checksum)
                                throws IOException {
        try (FileOutputStream fos = new FileOutputStream(outputFile);
             CheckedOutputStream cos = new CheckedOutputStream(fos, checksum);
             BufferedOutputStream bos = new BufferedOutputStream(cos);
             ZipOutputStream zos = new ZipOutputStream(bos)) {
            ZipUtil.compressDirectoryToZipFile(rootDir, sourceDir, zos);
            zos.flush();
            fos.getFD().sync();
        }
    }

    private static void compressDirectoryToZipFile(String rootDir,
                                                   String sourceDir,
                                                   ZipOutputStream zos)
                                                   throws IOException {
        String dir = Paths.get(rootDir, sourceDir).toString();
        File[] files = new File(dir).listFiles();
        E.checkNotNull(files, "files");
        for (File file : files) {
            String child = Paths.get(sourceDir, file.getName()).toString();
            if (file.isDirectory()) {
                compressDirectoryToZipFile(rootDir, child, zos);
            } else {
                zos.putNextEntry(new ZipEntry(child));
                try (FileInputStream fis = new FileInputStream(file);
                     BufferedInputStream bis = new BufferedInputStream(fis)) {
                    IOUtils.copy(bis, zos);
                }
            }
        }
    }

    public static void decompress(String sourceFile, String outputDir,
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
            // Continue to read all remaining bytes(extra metadata of ZipEntry)
            // directly from the checked stream, Otherwise, the checksum value
            // maybe unexpected.
            //
            // See https://coderanch.com/t/279175/java/ZipInputStream
            IOUtils.copy(cis, NullOutputStream.NULL_OUTPUT_STREAM);
        }
    }
}
