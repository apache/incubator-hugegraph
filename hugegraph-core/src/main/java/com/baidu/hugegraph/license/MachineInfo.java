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

package com.baidu.hugegraph.license;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.baidu.hugegraph.HugeException;

public class MachineInfo {

    public List<String> getIpAddress() {
        List<String> result = null;
        List<InetAddress> inetAddresses = this.getLocalAllInetAddress();
        if (inetAddresses != null && inetAddresses.size() > 0) {
            result = inetAddresses.stream().map(InetAddress::getHostAddress)
                                  .distinct().map(String::toLowerCase)
                                  .collect(Collectors.toList());
        }
        return result;
    }

    public List<String> getMacAddress() {
        List<String> result = null;
        List<InetAddress> inetAddresses = this.getLocalAllInetAddress();
        if (inetAddresses != null && inetAddresses.size() > 0) {
            // Get the Mac address of all network interfaces
            List<String> list = new ArrayList<>();
            Set<String> uniqueValues = new HashSet<>();
            for (InetAddress inetAddress : inetAddresses) {
                String macByInetAddress = this.getMacByInetAddress(inetAddress);
                if (uniqueValues.add(macByInetAddress)) {
                    list.add(macByInetAddress);
                }
            }
            result = list;
        }
        return result;
    }

    public List<InetAddress> getLocalAllInetAddress() {
        Enumeration interfaces;
        try {
            interfaces = NetworkInterface.getNetworkInterfaces();
        } catch (SocketException e) {
            throw new HugeException("Failed to get network interfaces");
        }

        List<InetAddress> result = new ArrayList<>(4);
        while (interfaces.hasMoreElements()) {
            NetworkInterface iface = (NetworkInterface) interfaces.nextElement();
            for (Enumeration inetAddresses = iface.getInetAddresses();
                 inetAddresses.hasMoreElements(); ) {
                InetAddress inetAddr = (InetAddress) inetAddresses.nextElement();
                if (!inetAddr.isLoopbackAddress() &&
                    !inetAddr.isLinkLocalAddress() &&
                    !inetAddr.isMulticastAddress()) {
                    result.add(inetAddr);
                }
            }
        }
        return result;
    }

    public String getMacByInetAddress(InetAddress inetAddr) {
        byte[] mac;
        try {
            mac = NetworkInterface.getByInetAddress(inetAddr)
                                  .getHardwareAddress();
        } catch (SocketException e) {
            throw new HugeException("Failed to get hardware addresses");
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < mac.length; i++) {
            if (i != 0) {
                sb.append("-");
            }
            String temp = Integer.toHexString(mac[i] & 0xff);
            if (temp.length() == 1) {
                sb.append("0").append(temp);
            } else {
                sb.append(temp);
            }
        }
        return sb.toString().toUpperCase();
    }
}
