/*
 * Copyright 2017 HugeGraph Authors
 *
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

package com.baidu.hugegraph.unit.license;

import java.io.IOException;

import org.junit.Test;

import com.baidu.hugegraph.license.LicenseInstallParam;
import com.baidu.hugegraph.testutil.Assert;
import com.fasterxml.jackson.databind.ObjectMapper;

public class LicenseInstallParamTest {

    @Test
    public void testDeserializeLicenseVerifyParam() throws IOException {
        String json = "{"
                + "\"subject\":\"hugegraph-evaluation\","
                + "\"public_alias\":\"publiccert\","
                + "\"store_ticket\":\"a123456\","
                + "\"publickey_path\":\"./publicCerts.store\","
                + "\"license_path\":\"./hugegraph-evaluation.license\""
                + "}";
        ObjectMapper mapper = new ObjectMapper();
        LicenseInstallParam param = mapper.readValue(json,
                                                    LicenseInstallParam.class);
        Assert.assertEquals("hugegraph-evaluation", param.subject());
        Assert.assertEquals("publiccert", param.publicAlias());
        Assert.assertEquals("a123456", param.storePassword());
        Assert.assertEquals("./publicCerts.store", param.publicKeyPath());
        Assert.assertEquals("./hugegraph-evaluation.license",
                            param.licensePath());
    }
}
