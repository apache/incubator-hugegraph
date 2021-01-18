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

package com.baidu.hugegraph.unit.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.zip.Checksum;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import com.alipay.sofa.jraft.util.CRC64;
import com.baidu.hugegraph.util.CompressUtil;
import com.google.common.collect.ImmutableList;

import junit.framework.AssertionFailedError;

public class CompressUtilTest {

    @Test
    public void testZipCompress() throws IOException {
        String rootDir = "temp";
        // temp/ss
        String sourceDir = "ss";
        String zipFile = "output.zip";
        String output = "output";
        try {
            prepareFiles(rootDir, sourceDir);

            Checksum checksum = new CRC64();
            CompressUtil.zipCompress(rootDir, sourceDir, zipFile, checksum);

            CompressUtil.zipDecompress(zipFile, output, checksum);
            assertDirEquals(rootDir, output);
        } finally {
            FileUtils.deleteQuietly(new File(rootDir));
            FileUtils.deleteQuietly(new File(zipFile));
            FileUtils.deleteQuietly(new File(output));
        }
    }

    @Test
    public void testTarCompress() throws IOException {
        String rootDir = "temp";
        // temp/ss
        String sourceDir = "ss";
        String tarFile = "output.tar";
        String output = "output";
        try {
            prepareFiles(rootDir, sourceDir);

            Checksum checksum = new CRC64();
            CompressUtil.tarCompress(rootDir, sourceDir, tarFile, checksum);

            CompressUtil.tarDecompress(tarFile, output, checksum);
            assertDirEquals(rootDir, output);
        } finally {
            FileUtils.deleteQuietly(new File(rootDir));
            FileUtils.deleteQuietly(new File(tarFile));
            FileUtils.deleteQuietly(new File(output));
        }
    }

    private static void prepareFiles(String rootDir, String sourceDir)
                                     throws IOException {
        // temp/ss/g
        String gDir = Paths.get(rootDir, sourceDir, "g").toString();
        File file1 = new File(gDir, "file-1");
        FileUtils.writeLines(file1, ImmutableList.of("g1-aaa", "g1-bbb"));
        File file2 = new File(gDir, "file-2");
        FileUtils.writeLines(file2, ImmutableList.of("g2-aaa", "g2-bbb"));
        // temp/ss/m
        String mDir = Paths.get(rootDir, sourceDir, "m").toString();
        file1 = new File(mDir, "file-1");
        FileUtils.writeLines(file1, ImmutableList.of("m1-aaa", "m1-bbb"));
        file2 = new File(mDir, "file-2");
        FileUtils.writeLines(file2, ImmutableList.of("m2-aaa", "m2-bbb"));
        // temp/ss/s
        String sDir = Paths.get(rootDir, sourceDir, "s").toString();
        file1 = new File(sDir, "file-1");
        FileUtils.writeLines(file1, ImmutableList.of("s1-aaa", "s1-bbb"));
        file2 = new File(sDir, "file-2");
        FileUtils.writeLines(file2, ImmutableList.of("s2-aaa", "s2-bbb"));
    }

    private static void assertDirEquals(String expect, String actual)
                                        throws IOException {
        Path expectDir = Paths.get(expect);
        Path actualDir = Paths.get(actual);
        Files.walkFileTree(expectDir, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file,
                                             BasicFileAttributes attrs)
                                             throws IOException {
                FileVisitResult result = super.visitFile(file, attrs);

                // Get the relative file name from path "one"
                Path relativize = expectDir.relativize(file);
                // Construct the path for the counterpart file in "other"
                Path fileInOther = actualDir.resolve(relativize);

                byte[] theseBytes = Files.readAllBytes(file);
                byte[] otherBytes = Files.readAllBytes(fileInOther);
                if (!Arrays.equals(theseBytes, otherBytes)) {
                    throw new AssertionFailedError(file + " is not equal to " +
                                                   fileInOther);
                }
                return result;
            }
        });
    }
}
