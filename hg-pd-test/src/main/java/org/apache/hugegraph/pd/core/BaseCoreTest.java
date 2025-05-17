package org.apache.hugegraph.pd.core;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.pd.BaseTest;
import org.junit.After;
import org.junit.BeforeClass;

import org.apache.hugegraph.pd.ConfigService;
import org.apache.hugegraph.pd.config.PDConfig;


public class BaseCoreTest extends BaseTest {

    static PDConfig pdConfig;

    static ConfigService configService;

    @BeforeClass
    public static void init() throws Exception {
        String path = "tmp/unitTest";
        deleteDirectory(new File(path));
        pdConfig = new PDConfig() {{
            this.setClusterId(100);
            this.setInitialStoreList("127.0.0.1:8500,127.0.0.1:8501,127.0.0.1:8502," +
                                     "127.0.0.1:8503,127.0.0.1:8504,127.0.0.1:8505");
        }};

        pdConfig.setStore(new PDConfig().new Store() {{
            this.setMaxDownTime(3600);
            this.setKeepAliveTimeout(3600);
        }});

        pdConfig.setPartition(new PDConfig().new Partition() {{
            this.setShardCount(3);
            this.setMaxShardsPerStore(3);
        }});
        pdConfig.setRaft(new PDConfig().new Raft() {{
            this.setEnable(false);
        }});
        pdConfig.setDiscovery(new PDConfig().new Discovery());
        pdConfig.setDataPath(path);
        configService = new ConfigService(pdConfig);
        pdConfig = configService.loadConfig();
    }

    public static void deleteDirectory(File dir) {
        try {
            FileUtils.deleteDirectory(dir);
        } catch (IOException e) {
            System.out.println(String.format("Failed to start ....,%s", e.getMessage()));
        }
    }

    @After
    public void teardown() {
        // pass
    }
}