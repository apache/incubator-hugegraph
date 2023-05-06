package org.apache.hugegraph.pd.service;

import com.baidu.hugegraph.pd.KvService;
import com.baidu.hugegraph.pd.config.PDConfig;
import org.junit.Assert;
import org.junit.Test;

public class KvServiceTest {

    @Test
    public void testKv() {
        try {
            PDConfig pdConfig = BaseServerTest.getConfig();
            KvService service = new KvService(pdConfig);
            String key = "kvTest";
            String kvTest = service.get(key);
            Assert.assertEquals(kvTest, "");
            service.put(key, "kvTestValue");
            kvTest = service.get(key);
            Assert.assertEquals(kvTest, "kvTestValue");
            service.scanWithPrefix(key);
            service.delete(key);
            service.put(key, "kvTestValue");
            service.deleteWithPrefix(key);
            service.put(key, "kvTestValue", 1000L);
            service.keepAlive(key);
        } catch (Exception e) {

        }
    }

    @Test
    public void testMember() {
        try {
            PDConfig pdConfig = BaseServerTest.getConfig();
            KvService service = new KvService(pdConfig);
            service.setPdConfig(pdConfig);
            PDConfig config = service.getPdConfig();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
