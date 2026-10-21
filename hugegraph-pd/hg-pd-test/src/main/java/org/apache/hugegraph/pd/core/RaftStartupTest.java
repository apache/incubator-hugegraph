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

package org.apache.hugegraph.pd.core;

import java.io.File;
import java.lang.reflect.Field;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.meta.MetadataFactory;
import org.apache.hugegraph.pd.raft.RaftEngine;
import org.apache.hugegraph.pd.service.RaftStartup;
import org.junit.Assert;
import org.junit.Test;

public class RaftStartupTest {

    @Test
    public void testRaftDoesNotStartBeforeMetadataOpens() throws Exception {
        Assert.assertNull(RaftEngine.getInstance().getRaftNode());
        String dataPath = "tmp/test-raft-startup";
        FileUtils.deleteQuietly(new File(dataPath));
        // A regular file where the metadata RocksDB directory belongs makes its open fail
        FileUtils.forceMkdir(new File(dataPath));
        FileUtils.touch(new File(dataPath, "rocksdb"));
        PDConfig config = new PDConfig();
        config.setDataPath(dataPath);
        PDConfig.Raft raft = config.new Raft();
        raft.setDataPath(dataPath + "/raft");
        config.setRaft(raft);

        Field storeField = MetadataFactory.class.getDeclaredField("store");
        Field shutdownField = MetadataFactory.class.getDeclaredField("shutdown");
        storeField.setAccessible(true);
        shutdownField.setAccessible(true);
        synchronized (MetadataFactory.class) {
            Object previous = storeField.get(null);
            boolean shutdown = shutdownField.getBoolean(null);
            try {
                storeField.set(null, null);
                shutdownField.setBoolean(null, false);
                IllegalStateException e = Assert.assertThrows(IllegalStateException.class,
                                                              () -> RaftStartup.init(config));
                Assert.assertTrue(e.getMessage(),
                                  e.getMessage().startsWith("Failed to open PD metadata"));
                Assert.assertFalse(new File(dataPath, "raft").exists());
                Assert.assertNull(RaftEngine.getInstance().getRaftNode());
            } finally {
                storeField.set(null, previous);
                shutdownField.setBoolean(null, shutdown);
                FileUtils.deleteQuietly(new File(dataPath));
            }
        }
    }
}
