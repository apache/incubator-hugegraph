package org.apache.hugegraph.pd.service;

import java.io.File;
import java.net.http.HttpClient;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.BeforeClass;

import org.apache.hugegraph.pd.BaseTest;
import org.apache.hugegraph.pd.config.PDConfig;


public class BaseServerTest extends BaseTest {

    public static HttpClient client;

    @BeforeClass
    public static void init() {
        client = HttpClient.newHttpClient();
    }

    public static PDConfig getConfig() {
        FileUtils.deleteQuietly(new File("tmp/test/"));
        PDConfig pdConfig = new PDConfig() {{
            this.setClusterId(100);
            this.setPatrolInterval(1);
            this.setRaft(new Raft() {{
                setEnable(false);
            }});
            this.setPartition(new Partition() {{
                setShardCount(1);
                setMaxShardsPerStore(12);
            }});
            this.setDataPath("tmp/test/");
        }};
        return pdConfig;
    }

    @After
    public void teardown() {
        // pass
    }

}