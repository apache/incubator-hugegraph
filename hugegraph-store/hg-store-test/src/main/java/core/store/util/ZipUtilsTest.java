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

package core.store.util;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.zip.CRC32;

import org.apache.hugegraph.store.util.ZipUtils;
import org.apache.logging.log4j.core.util.FileUtils;
import org.junit.Before;
import org.junit.Test;

import util.UnitTestBase;

public class ZipUtilsTest {
    private static final String ZIP_TEST_PATH = "/tmp/zip_util_test";

    @Before
    public void init() throws IOException {
        UnitTestBase.deleteDir(new File(ZIP_TEST_PATH));
        FileUtils.mkdir(new File(ZIP_TEST_PATH), true);
        FileUtils.mkdir(new File(ZIP_TEST_PATH + "/input"), true);
        FileUtils.mkdir(new File(ZIP_TEST_PATH + "/output"), true);
        Files.createFile(Paths.get(ZIP_TEST_PATH + "/input/foo.txt"));
    }

    @Test
    public void testZip() throws IOException {
        ZipUtils.compress(ZIP_TEST_PATH, "input", ZIP_TEST_PATH + "/foo.zip", new CRC32());
        ZipUtils.decompress(ZIP_TEST_PATH + "/foo.zip", ZIP_TEST_PATH + "/output", new CRC32());
        assertTrue(Files.exists(Paths.get(ZIP_TEST_PATH + "/output/input/foo.txt")));
    }
}
