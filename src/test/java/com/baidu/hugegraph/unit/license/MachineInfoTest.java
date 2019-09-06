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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.regex.Pattern;

import org.junit.Test;

import com.baidu.hugegraph.license.MachineInfo;
import com.baidu.hugegraph.testutil.Assert;

public class MachineInfoTest {

    private static final Pattern IPV4_PATTERN = Pattern.compile(
            "^(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}" +
            "([01]?\\d\\d?|2[0-4]\\d|25[0-5])$"
    );
    private static final Pattern IPV6_PATTERN = Pattern.compile(
            "^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$"
    );

    private static final Pattern MAC_PATTERN = Pattern.compile(
            "^([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})$"
    );

    private static final MachineInfo machineInfo = new MachineInfo();

    @Test
    public void testGetIpAddressList() {
        List<String> ipAddressList = machineInfo.getIpAddress();
        for (String ip : ipAddressList) {
            Assert.assertTrue(IPV4_PATTERN.matcher(ip).matches() ||
                              IPV6_PATTERN.matcher(ip).matches());
        }
        Assert.assertEquals(ipAddressList, machineInfo.getIpAddress());
    }

    @Test
    public void testGetMacAddressList() {
        List<String> macAddressList = machineInfo.getMacAddress();
        for (String mac : macAddressList) {
            Assert.assertTrue(MAC_PATTERN.matcher(mac).matches());
        }
        Assert.assertEquals(macAddressList, machineInfo.getMacAddress());
    }

    @Test
    public void testGetLocalAllInetAddress() {
        List<InetAddress> addressList = machineInfo.getLocalAllInetAddress();
        for (InetAddress address : addressList) {
            String ip = address.getHostAddress();
            Assert.assertTrue(IPV4_PATTERN.matcher(ip).matches() ||
                              IPV6_PATTERN.matcher(ip).matches());
        }
    }

    @Test
    public void testGetMacByInetAddress() throws UnknownHostException {
        List<InetAddress> addressList = machineInfo.getLocalAllInetAddress();
        for (InetAddress address : addressList) {
            String mac = machineInfo.getMacByInetAddress(address);
            Assert.assertTrue(MAC_PATTERN.matcher(mac).matches());
        }
        InetAddress address = InetAddress.getByAddress(new byte[]{0, 0, 0, 0});
        Assert.assertThrows(RuntimeException.class, () -> {
            machineInfo.getMacByInetAddress(address);
        });
    }
}
