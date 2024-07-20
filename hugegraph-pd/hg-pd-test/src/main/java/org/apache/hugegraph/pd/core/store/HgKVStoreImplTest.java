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

package org.apache.hugegraph.pd.core.store;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.store.HgKVStore;
import org.apache.hugegraph.pd.store.HgKVStoreImpl;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class HgKVStoreImplTest {

    private static final String testPath = "tmp/test";
    private static PDConfig pdConfig;

    @BeforeClass
    public static void init() throws IOException {
        File testFile = new File(testPath);
        if (testFile.exists()) {
            FileUtils.deleteDirectory(testFile);
        }
        FileUtils.forceMkdir(testFile);
        pdConfig = new PDConfig() {{
            setDataPath(testPath);
        }};
    }

    @Test
    public void Test() throws PDException {
        HgKVStore kvStore = new HgKVStoreImpl();
        kvStore.init(pdConfig);

        {
            byte[] key = "hello".getBytes();
            byte[] value = "pd".getBytes();
            kvStore.put(key, value);
        }
        for (int i = 0; i < 100; i++) {
            byte[] key = String.format("k%03d", i).getBytes();
            byte[] value = ("value" + i).getBytes();
            kvStore.put(key, value);
        }

        Assert.assertEquals(100, kvStore.scanPrefix("k".getBytes()).size());

        kvStore.removeByPrefix("k".getBytes());
        Assert.assertEquals(0, kvStore.scanPrefix("k".getBytes()).size());

        kvStore.close();
    }

    @Test
    public void TestSnapshot() throws PDException {
        HgKVStore kvStore = new HgKVStoreImpl();
        kvStore.init(pdConfig);

        // put 100 data
        for (int i = 0; i < 100; i++) {
            byte[] key = String.format("k%03d", i).getBytes();
            byte[] value = ("value" + i).getBytes();
            kvStore.put(key, value);
        }
        Assert.assertEquals(100, kvStore.scanPrefix("k".getBytes()).size());

        // save snapshot
        String snapshotPath = Paths.get(testPath, "snapshot").toString();
        kvStore.saveSnapshot(snapshotPath);

        // put another 100 data
        for (int i = 100; i < 200; i++) {
            byte[] key = String.format("k%03d", i).getBytes();
            byte[] value = ("value" + i).getBytes();
            kvStore.put(key, value);
        }
        Assert.assertEquals(200, kvStore.scanPrefix("k".getBytes()).size());

        // load snapshot
        kvStore.loadSnapshot(snapshotPath);
        Assert.assertEquals(100, kvStore.scanPrefix("k".getBytes()).size());

        // put another 100 data
        for (int i = 100; i < 200; i++) {
            byte[] key = String.format("k%03d", i).getBytes();
            byte[] value = ("value" + i).getBytes();
            kvStore.put(key, value);
        }
        Assert.assertEquals(200, kvStore.scanPrefix("k".getBytes()).size());

        kvStore.close();
    }
}
