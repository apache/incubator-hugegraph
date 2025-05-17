package org.apache.hugegraph.pd.upgrade;

import org.apache.hugegraph.pd.config.PDConfig;

public interface VersionUpgradeScript {

    String UNLIMITED_VERSION = "UNLIMITED_VERSION";

    /**
     * the highest version that need to run upgrade instruction
     * @return high version
     */
    String getHighVersion();

    /**
     * the lowest version that need to run upgrade instruction
     * @return lower version
     */
    String getLowVersion();

    /**
     * pd中没有data version的时候，是否执行. 一般是对应3。6。2之前的版本
     *
     * @return run when pd has no data version
     */
    boolean isRunWithoutDataVersion();

    /**
     * the scrip just run once, ignore versions
     * @return run once script
     */
    boolean isRunOnce();

    /**
     * run the upgrade instruction
     */
    void runInstruction(PDConfig config);

}
