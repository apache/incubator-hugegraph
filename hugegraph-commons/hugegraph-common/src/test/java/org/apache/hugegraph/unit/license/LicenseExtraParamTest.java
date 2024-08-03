/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.unit.license;

import java.io.IOException;

import org.apache.hugegraph.license.LicenseExtraParam;
import org.junit.Test;

import org.apache.hugegraph.testutil.Assert;
import com.fasterxml.jackson.databind.ObjectMapper;

public class LicenseExtraParamTest {

    @Test
    public void testDeserializeExtraParam() throws IOException {
        String json = "{"
                + "\"id\":\"server-1\","
                + "\"version\":\"0.10.2\","
                + "\"graphs\":3,"
                + "\"ip\":\"127.0.0.1\","
                + "\"mac\":\"00-01-6C-06-A6-29\","
                + "\"cpus\":32,"
                + "\"ram\":65536,"
                + "\"threads\":96,"
                + "\"memory\":32768,"
                + "\"nodes\":3,"
                + "\"data_size\":1024,"
                + "\"vertices\":1000,"
                + "\"edges\":2000"
                + "}";
        ObjectMapper mapper = new ObjectMapper();
        LicenseExtraParam param = mapper.readValue(json, LicenseExtraParam.class);
        Assert.assertEquals("server-1", param.id());
        Assert.assertEquals("0.10.2", param.version());
        Assert.assertEquals(3, param.graphs());
        Assert.assertEquals("127.0.0.1", param.ip());
        Assert.assertEquals("00-01-6C-06-A6-29", param.mac());
        Assert.assertEquals(32, param.cpus());
        Assert.assertEquals(65536, param.ram());
        Assert.assertEquals(96, param.threads());
        Assert.assertEquals(32768, param.memory());
        Assert.assertEquals(3, param.nodes());
        Assert.assertEquals(1024, param.dataSize());
        Assert.assertEquals(1000, param.vertices());
        Assert.assertEquals(2000, param.edges());
    }
}
