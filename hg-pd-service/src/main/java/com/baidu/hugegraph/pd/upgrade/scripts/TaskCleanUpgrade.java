package com.baidu.hugegraph.pd.upgrade.scripts;

import com.baidu.hugegraph.pd.common.PDException;
import com.baidu.hugegraph.pd.config.PDConfig;
import com.baidu.hugegraph.pd.meta.MetadataKeyHelper;
import com.baidu.hugegraph.pd.meta.MetadataRocksDBStore;
import com.baidu.hugegraph.pd.upgrade.VersionUpgradeScript;
import lombok.extern.slf4j.Slf4j;

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
