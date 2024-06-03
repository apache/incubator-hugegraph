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

package org.apache.hugegraph.store.util;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IpUtil {

    /**
     * 获取所有的ipv4 地址
     *
     * @return all ipv4 addr
     * @throws SocketException io error or no network interface
     */
    private static List<String> getIpAddress() throws SocketException {
        List<String> list = new LinkedList<>();
        Enumeration enumeration = NetworkInterface.getNetworkInterfaces();
        while (enumeration.hasMoreElements()) {
            NetworkInterface network = (NetworkInterface) enumeration.nextElement();
            Enumeration addresses = network.getInetAddresses();
            while (addresses.hasMoreElements()) {
                InetAddress address = (InetAddress) addresses.nextElement();
                if (address != null && (address instanceof Inet4Address)) {
                    list.add(address.getHostAddress());
                }
            }
        }
        return list;
    }

    /**
     * 根据 option中的raft addr，根据本机的ip获取最相近的一个
     *
     * @param raftAddress raft addr
     * @return raft addr that have the nearest distance with given param
     */
    public static String getNearestAddress(String raftAddress) {
        try {
            List<String> ipv4s = getIpAddress();
            String[] tmp = raftAddress.split(":");
            if (ipv4s.size() == 0) {
                throw new Exception("no available ipv4");
            }

            if (ipv4s.size() == 1) {
                return ipv4s.get(0) + ":" + tmp[1];
            }

            var raftSeg = Arrays.stream(tmp[0].split("\\."))
                                .map(s -> Integer.parseInt(s))
                                .collect(Collectors.toList());

            ipv4s.sort(Comparator.comparingLong(ip -> {
                String[] ipSegments = ip.split("\\.");
                long base = 256 * 256 * 256;
                int i = 0;
                long sum = 0;
                for (String seg : ipSegments) {
                    sum += base * (Math.abs(raftSeg.get(i) - Integer.parseInt(seg)));
                    base = base / 256;
                    i += 1;
                }
                return sum;
            }));

            return ipv4s.get(0) + ":" + tmp[1];
        } catch (SocketException e) {
            log.error("getIpAddress, get ip failed, {}", e.getMessage());
        } catch (Exception e) {
            log.error("getRaftAddress, got exception, {}", e.getMessage());
        }
        return raftAddress;
    }
}
