package org.apache.hugegraph.pd.upgrade.scripts;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.meta.MetadataKeyHelper;
import org.apache.hugegraph.pd.meta.MetadataRocksDBStore;
import org.apache.hugegraph.pd.upgrade.VersionUpgradeScript;
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
