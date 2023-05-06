package org.apache.hugegraph.pd.core;

import com.baidu.hugegraph.pd.ConfigService;
import com.baidu.hugegraph.pd.config.PDConfig;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;


public class BaseCoreTest {

    static com.baidu.hugegraph.pd.config.PDConfig pdConfig;
    @BeforeClass
    public static void init() throws Exception {
        String path = "tmp/unitTest";
        deleteDirectory(new File(path));
        pdConfig = new com.baidu.hugegraph.pd.config.PDConfig() {{
            this.setClusterId(100);
            this.setInitialStoreList("127.0.0.1:8500,127.0.0.1:8501,127.0.0.1:8502," +
                    "127.0.0.1:8503,127.0.0.1:8504,127.0.0.1:8505");
        }};

        pdConfig.setStore(new com.baidu.hugegraph.pd.config.PDConfig().new Store() {{
            this.setMaxDownTime(3600);
            this.setKeepAliveTimeout(3600);
        }});

        pdConfig.setPartition(new com.baidu.hugegraph.pd.config.PDConfig().new Partition() {{
            this.setShardCount(3);
            this.setMaxShardsPerStore(3);
        }});
        pdConfig.setRaft(new com.baidu.hugegraph.pd.config.PDConfig().new Raft(){{
            this.setEnable(false);
        }});
        pdConfig.setDiscovery(new PDConfig().new Discovery());
        pdConfig.setDataPath(path);
        ConfigService configService = new ConfigService(pdConfig);
        pdConfig = configService.loadConfig();
    }

    @After
    public void teardown() throws Exception {
        // pass
    }

    public static void deleteDirectory(File dir) {
        try {
            FileUtils.deleteDirectory(dir);
        } catch (IOException e) {
            System.out.println(String.format("Failed to start ....,%s", e.getMessage()));
        }
    }
}