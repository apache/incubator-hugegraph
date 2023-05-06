package org.apache.hugegraph.pd.service;

import com.baidu.hugegraph.pd.config.PDConfig;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.BeforeClass;

import java.net.http.HttpClient;
import java.io.File;


public class BaseServerTest {
    public static HttpClient client;
    public static String pdRestAddr;
    @BeforeClass
    public static void init()  {
       client = HttpClient.newHttpClient();
       pdRestAddr = "http://127.0.0.1:8620";
    }

    @After
    public void teardown() {
        // pass
    }

    public static PDConfig getConfig(){
        FileUtils.deleteQuietly(new File("tmp/test/"));
        PDConfig pdConfig = new PDConfig() {{
            this.setClusterId(100);
            this.setPatrolInterval(1);
            this.setRaft(new Raft() {{
                setEnable(false);
            }});
            this.setDataPath("tmp/test/");
        }};
        return pdConfig;
    }

}