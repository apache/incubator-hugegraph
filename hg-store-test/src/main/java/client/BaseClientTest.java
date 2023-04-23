package client;

import com.baidu.hugegraph.pd.client.PDClient;
import com.baidu.hugegraph.pd.client.PDConfig;
import com.baidu.hugegraph.store.HgStoreClient;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BaseClientTest {
    public static HgStoreClient storeClient;

    public static PDClient pdClient;
    public static String tableName0 = "cli-table0";
    protected static String graphName = "testGraphName";
    protected static String tableName = "testTableName";

    @BeforeClass
    public static void beforeClass() {
        storeClient = HgStoreClient.create(PDConfig.of("127.0.0.1:8686")
                .setEnableCache(true));
        pdClient = storeClient.getPdClient();
    }

    @After
    public void teardown() {
        // pass
    }
}