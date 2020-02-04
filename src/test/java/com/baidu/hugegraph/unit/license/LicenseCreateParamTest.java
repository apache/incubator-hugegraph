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
import java.text.DateFormat;
import java.text.ParseException;
import java.util.TimeZone;

import org.junit.Test;

import com.baidu.hugegraph.date.SafeDateFormat;
import com.baidu.hugegraph.license.LicenseCreateParam;
import com.baidu.hugegraph.testutil.Assert;
import com.fasterxml.jackson.databind.ObjectMapper;

public class LicenseCreateParamTest {

    @Test
    public void testDeserializeLicenseCreateParam()
           throws IOException, ParseException {
        String json = "{"
                + "\"subject\":\"hugegraph-evaluation\","
                + "\"private_alias\":\"privatekey\","
                + "\"key_password\":\"123456\","
                + "\"store_password\":\"123456\","
                + "\"privatekey_path\":\"./privateKeys.store\","
                + "\"license_path\":\"./hugegraph-evaluation.license\","
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
        LicenseCreateParam param = mapper.readValue(json,
                                                    LicenseCreateParam.class);
        Assert.assertEquals("hugegraph-evaluation", param.subject());
        Assert.assertEquals("privatekey", param.privateAlias());
        Assert.assertEquals("123456", param.keyPassword());
        Assert.assertEquals("123456", param.storePassword());
        Assert.assertEquals("./privateKeys.store", param.privateKeyPath());
        Assert.assertEquals("./hugegraph-evaluation.license",
                            param.licensePath());

        DateFormat df = new SafeDateFormat("yyyy-MM-dd HH:mm:ss");
        df.setTimeZone(TimeZone.getTimeZone("GMT+08:00"));
        Assert.assertEquals(df.parse("2019-08-10 00:00:00"),
                            param.issuedTime());
        Assert.assertEquals(df.parse("2019-08-10 00:00:00"),
                            param.notBefore());
        Assert.assertEquals(df.parse("2020-08-10 00:00:00"),
                            param.notAfter());
        Assert.assertEquals("user", param.consumerType());
        Assert.assertEquals(1, param.consumerAmount());
        Assert.assertEquals("hugegraph license", param.description());
        Assert.assertEquals(2, param.extraParams().size());
    }
}
