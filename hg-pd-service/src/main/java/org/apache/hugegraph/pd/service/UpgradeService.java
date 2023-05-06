package com.baidu.hugegraph.pd.service;

import com.baidu.hugegraph.pd.KvService;
import com.baidu.hugegraph.pd.common.PDException;
import com.baidu.hugegraph.pd.config.PDConfig;
import com.baidu.hugegraph.pd.rest.API;
import com.baidu.hugegraph.pd.upgrade.VersionScriptFactory;
import com.baidu.hugegraph.pd.upgrade.VersionUpgradeScript;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpgradeService {

    private static final String VERSION_KEY = "DATA_VERSION";

    private static final String RUN_LOG_PREFIX = "SCRIPT_RUN_LOG";

    private PDConfig pdConfig;

    private KvService kvService;

    public UpgradeService (PDConfig pdConfig){
        this.pdConfig = pdConfig;
        this.kvService = new KvService(pdConfig);
    }

    public void upgrade() throws PDException {

        log.info("upgrade service start");
        VersionScriptFactory factory = VersionScriptFactory.getInstance();
        var dataVersion = getDataVersion();
        log.info("now db data version : {}", dataVersion);
        for(VersionUpgradeScript script : factory.getScripts()) {
            // 执行过，run once的跳过
            if (isExecuted(script.getClass().getName()) && script.isRunOnce()) {
                log.info("Script {} is Executed and is run once", script.getClass().getName());
                continue;
            }

            // 判断跳过的条件
            if (dataVersion == null && !script.isRunWithoutDataVersion() || dataVersion != null &&
                            !versionCompare(dataVersion, script.getHighVersion(), script.getLowVersion())) {
                log.info("Script {} is did not match version requirements, current data version:{}, current version:{}"
                        + "script run version({} to {}), run without data version:{}",
                        script.getClass().getName(),
                        dataVersion,
                        API.VERSION,
                        script.getHighVersion(),
                        script.getLowVersion(),
                        script.isRunWithoutDataVersion());
                continue;
            }

            script.runInstruction(pdConfig);
            logRun(script.getClass().getName());
        }

        writeCurrentDataVersion();
    }

    private boolean isExecuted(String className) throws PDException {
        var ret = kvService.get(RUN_LOG_PREFIX + "/" + className);
        return ret.length() > 0;
    }

    private void logRun(String className) throws PDException {
        kvService.put(RUN_LOG_PREFIX + "/" + className, API.VERSION);
    }

    private String getDataVersion() throws PDException {
        return kvService.get(VERSION_KEY);
    }

    private boolean versionCompare(String dataVersion, String high, String low) {
        var currentVersion = API.VERSION;
        if (!high.equals(VersionUpgradeScript.UNLIMITED_VERSION) && high.compareTo(dataVersion) < 0
            || !low.equals(VersionUpgradeScript.UNLIMITED_VERSION) && low.compareTo(currentVersion) > 0){
            return false;
        }
        return true;
    }

    private void writeCurrentDataVersion() throws PDException {
        log.info("update db version to {}", API.VERSION);
        kvService.put(VERSION_KEY, API.VERSION);
    }

}
