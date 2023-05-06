package org.apache.hugegraph.pd.client;

import com.baidu.hugegraph.pd.client.PDClient;
import com.baidu.hugegraph.pd.client.PDConfig;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BaseClientTest {
    public static PDClient pdClient;

    public long storeId = 0;
    public final String storeAddr = "localhost";
    public final String graphName = "default/hugegraph/g";

    @BeforeClass
    public static void beforeClass() throws Exception {
        PDConfig config = PDConfig.of("localhost:8686");
//        PDConfig config = PDConfig.of("10.81.116.77:8986");
        config.setEnableCache(true);
        pdClient = PDClient.create(config);
    }

    @After
    public void teardown() throws Exception {
        // pass
    }
}