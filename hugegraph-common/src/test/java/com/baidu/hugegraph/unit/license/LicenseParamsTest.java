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

import com.baidu.hugegraph.license.LicenseExtraParam;
import com.baidu.hugegraph.license.LicenseParams;
import com.baidu.hugegraph.testutil.Assert;
import com.fasterxml.jackson.databind.ObjectMapper;

public class LicenseParamsTest {

    @Test
    public void testLicenseParams() throws IOException {
        String json = "{"
                + "\"subject\":\"hugegraph-evaluation\","
                + "\"issued_time\":\"2019-08-10 00:00:00\","
                + "\"not_before\":\"2019-08-10 00:00:00\","
                + "\"not_after\":\"2020-08-10 00:00:00\","
                + "\"consumer_type\":\"user\","
                + "\"consumer_amount\":1,"
                + "\"description\":\"hugegraph license\","
                + "\"extra_params\":["
                + "{"
                + "\"id\":\"server-1\","
                + "\"version\":\"0.9.2\","
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
                + "},"
                + "{"
                + "\"id\":\"server-2\","
                + "\"version\":\"0.10.2\","
                + "\"graphs\":3,"
                + "\"ip\":\"127.0.0.1\","
                + "\"mac\":\"00-02-6C-06-A6-29\","
                + "\"cpus\":64,"
                + "\"ram\":65536,"
                + "\"threads\":96,"
                + "\"memory\":65536,"
                + "\"nodes\":30,"
                + "\"data_size\":10240,"
                + "\"vertices\":10000,"
                + "\"edges\":20000"
                + "}"
                + "]"
                + "}";
        ObjectMapper mapper = new ObjectMapper();
        LicenseParams param = mapper.readValue(json, LicenseParams.class);

        LicenseExtraParam extraParam = param.matchParam("server-not-exist");
        Assert.assertNull(extraParam);

        extraParam = param.matchParam("server-1");
        Assert.assertEquals("00-01-6C-06-A6-29", extraParam.mac());

        extraParam = param.matchParam("server-2");
        Assert.assertEquals("00-02-6C-06-A6-29", extraParam.mac());
    }
}
