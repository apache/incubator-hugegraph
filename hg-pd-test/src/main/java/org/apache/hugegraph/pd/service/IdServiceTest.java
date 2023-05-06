package org.apache.hugegraph.pd.service;

import com.baidu.hugegraph.pd.IdService;
import com.baidu.hugegraph.pd.config.PDConfig;
import com.baidu.hugegraph.pd.meta.IdMetaStore;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class IdServiceTest {
     @Test
    public void testCid() {
         try{
             PDConfig pdConfig = BaseServerTest.getConfig();
             int max = 0x2000;
             IdService idService = new IdService(pdConfig);
             for (int i = 0; i < max; i++) {
                 idService.getCId("test", max);
             }
             idService.delCId("test", 1);
             idService.delCId("test", 0x10);
             idService.delCId("test", 0x100);
             idService.delCId("test", 0x1000);

             Assert.assertEquals(1, idService.getCId("test", max));
             Assert.assertEquals(0x10, idService.getCId("test", max));
             Assert.assertEquals(0x100, idService.getCId("test", max));
             Assert.assertEquals(0x1000, idService.getCId("test", max));
             Assert.assertEquals(-1, idService.getCId("test", max));

             idService.delCId("test", 1);
             idService.delCId("test", 0x10);
             idService.delCId("test", 0x100);
             idService.delCId("test", 0x1000);

             long cid1 = idService.getCId("test", "name", max);
             idService.delCIdDelay("test", "name", cid1);
             long cid2 = idService.getCId("test", "name", max);

             Assert.assertEquals(cid1, cid2);
             idService.delCIdDelay("test", "name", cid2);
             Thread.sleep(5000);
             long cid3 = idService.getCId("test", "name", max);
         } catch (Exception e) {

         }
         // MetadataFactory.closeStore();
    }

    @Test
    public void testId() {
         try{
             FileUtils.deleteQuietly(new File("tmp/testId/"));
             IdMetaStore.CID_DEL_TIMEOUT = 2000;
             PDConfig pdConfig = new PDConfig() {{
                 this.setClusterId(100);
                 this.setPatrolInterval(1);
                 this.setRaft(new Raft() {{
                     setEnable(false);
                 }});
                 this.setDataPath("tmp/testId/");
             }};
             IdService idService = new IdService(pdConfig);
             long first = idService.getId("abc", 100);
             Assert.assertEquals(first, 0L);
             long second = idService.getId("abc", 100);
             Assert.assertEquals(second, 100L);
             idService.resetId("abc");
             first = idService.getId("abc", 100);
             Assert.assertEquals(first, 0L);
         } catch (Exception e) {

         }
        // MetadataFactory.closeStore();
    }
    @Test
    public void testMember() {
        try{
            PDConfig pdConfig = BaseServerTest.getConfig();
            IdService idService = new IdService(pdConfig);
            idService.setPdConfig(pdConfig);
            PDConfig config = idService.getPdConfig();
            config.getHost();
        } catch (Exception e) {
            e.printStackTrace();
        }
        // MetadataFactory.closeStore();
    }
}
