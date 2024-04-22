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

package org.apache.hugegraph.pd.upgrade.scripts;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.common.Useless;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.meta.MetadataKeyHelper;
import org.apache.hugegraph.pd.meta.MetadataRocksDBStore;
import org.apache.hugegraph.pd.upgrade.VersionUpgradeScript;

import lombok.extern.slf4j.Slf4j;

@Useless("upgrade related")
@Slf4j
public class TaskCleanUpgrade implements VersionUpgradeScript {

    @Override
    public String getHighVersion() {
        return UNLIMITED_VERSION;
    }

    @Override
    public String getLowVersion() {
        return UNLIMITED_VERSION;
    }

    @Override
    public boolean isRunWithoutDataVersion() {
        return true;
    }

    @Override
    public boolean isRunOnce() {
        return true;
    }

    @Override
    public void runInstruction(PDConfig config) {
        log.info("run TaskCleanUpgrade script");
        var dbStore = new MetadataRocksDBStore(config);

        try {
            byte[] key = MetadataKeyHelper.getAllSplitTaskPrefix();
            log.info("delete split task:{}", dbStore.removeByPrefix(key));
            byte[] key2 = MetadataKeyHelper.getAllMoveTaskPrefix();
            log.info("delete move task:{}", dbStore.removeByPrefix(key2));
        } catch (PDException e) {
            throw new RuntimeException(e);
        }

    }
}
