/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.pd.core;

import org.apache.hugegraph.pd.IdService;
import org.apache.hugegraph.pd.config.PDConfig;
import org.junit.Assert;
import org.junit.Test;

public class IdServiceTest extends PDCoreTestBase {

    @Test
    public void testCid() {
        try {
            PDConfig pdConfig = getPdConfig();
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
            e.printStackTrace();
        }
        // MetadataFactory.closeStore();
    }

    @Test
    public void testId() {
        try {
            PDConfig pdConfig = getPdConfig();
            IdService idService = new IdService(pdConfig);
            long first = idService.getId("abc", 100);
            Assert.assertEquals(first, 0L);
            long second = idService.getId("abc", 100);
            Assert.assertEquals(second, 100L);
            idService.resetId("abc");
            first = idService.getId("abc", 100);
            Assert.assertEquals(first, 0L);
        } catch (Exception e) {
            e.printStackTrace();
        }
        // MetadataFactory.closeStore();
    }

    @Test
    public void testMember() {
        try {
            PDConfig pdConfig = getPdConfig();
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
