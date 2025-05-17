package org.apache.hugegraph.pd.client;

import org.apache.hugegraph.pd.BaseTest;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BaseClientTest extends BaseTest {

    public static PDClient pdClient;
    public final String storeAddr = "localhost";
    public final String graphName = "default/hugegraph/g";
    public long storeId = 0;

    @BeforeClass
    public static void beforeClass() {
        PDConfig config = PDConfig.of(pdGrpcAddr).setAuthority(user, pwd);
        config.setEnableCache(true);
        pdClient = PDClient.create(config);
    }

    @After
    public void teardown() {
        // pass
    }
}