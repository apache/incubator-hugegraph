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

package org.apache.hugegraph.pd.raft.auth;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Set;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ChannelHandler.Sharable
public class IpAuthHandler extends ChannelDuplexHandler {

    private final Set<String> allowedIps;
    private static volatile IpAuthHandler instance;

    private IpAuthHandler(Set<String> allowedIps) {
        this.allowedIps = Collections.unmodifiableSet(allowedIps);
    }

    public static IpAuthHandler getInstance(Set<String> allowedIps) {
        if (instance == null) {
            synchronized (IpAuthHandler.class) {
                if (instance == null) {
                    instance = new IpAuthHandler(allowedIps);
                }
            }
        }
        return instance;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        String clientIp = getClientIp(ctx);
        if (!isIpAllowed(clientIp)) {
            log.warn("Blocked connection from {}", clientIp);
            ctx.close();
            return;
        }
        super.channelActive(ctx);
    }

    private static String getClientIp(ChannelHandlerContext ctx) {
        InetSocketAddress remoteAddress = (InetSocketAddress) ctx.channel().remoteAddress();
        return remoteAddress.getAddress().getHostAddress();
    }

    private boolean isIpAllowed(String ip) {
        return allowedIps.isEmpty() || allowedIps.contains(ip);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        String clientIp=getClientIp(ctx);
        log.warn("clien : {} connection exception : {}",clientIp,cause);
        if (ctx.channel().isActive()) {
            ctx.close().addListener(future -> {
                if (!future.isSuccess()) {
                    log.warn("clien : {} connection closed failed : {}",clientIp,future.cause().getMessage());
                }
            });
        }
    }

}
