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

package org.apache.hugegraph.pd.service.interceptor;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.lang3.StringUtils;
import org.apache.hugegraph.pd.KvService;
import org.apache.hugegraph.pd.common.Cache;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.util.TokenUtil;
import org.apache.hugegraph.util.StringEncoding;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.stereotype.Component;

@Component
public class Authentication {

    @Autowired
    private KvService kvService;
    @Autowired
    private PDConfig pdConfig;

    private static final Cache<String> TOKEN_CACHE = new Cache<>();
    private static volatile TokenUtil util;
    private static String invalidMsg =
            "invalid token and invalid user name or password, access denied";
    private static String invalidBasicInfo = "invalid basic authentication info";

    protected <T> T authenticate(String authority, String token, Function<String, T> tokenCall,
                                 Supplier<T> call) {
        try {
            if (StringUtils.isEmpty(authority)) {
                throw new BadCredentialsException(invalidBasicInfo);
            }
            byte[] bytes = authority.getBytes(StandardCharsets.UTF_8);
            byte[] decode = Base64.getDecoder().decode(bytes);
            String info = new String(decode);
            int delim = info.indexOf(':');
            if (delim == -1) {
                throw new BadCredentialsException(invalidBasicInfo);
            }
            String name = info.substring(0, delim);
            String pwd = info.substring(delim + 1);
            if (!"store".equals(name)) {
                if (util == null) {
                    synchronized (this) {
                        if (util == null) {
                            util = new TokenUtil(pdConfig.getSecretKey());
                        }
                    }
                }
                String[] i = util.getInfo(name);
                if (i == null) {
                    throw new AccessDeniedException("invalid service name");
                }
                if (!StringUtils.isEmpty(token)) {
                    String value = TOKEN_CACHE.get(name);
                    if (StringUtils.isEmpty(value)) {
                        synchronized (i) {
                            value = kvService.get(getTokenKey(name));
                        }
                    }
                    if (!StringUtils.isEmpty(value) && token.equals(value)) {
                        return call.get();
                    }
                }
                if (StringUtils.isEmpty(pwd) || !StringEncoding.checkPassword(i[2], pwd)) {
                    throw new AccessDeniedException(invalidMsg);
                }
                token = util.getToken(name);
                String tokenKey = getTokenKey(name);
                String dbToken = kvService.get(tokenKey);
                if (StringUtils.isEmpty(dbToken)) {
                    synchronized (i) {
                        dbToken = kvService.get(tokenKey);
                        if (StringUtils.isEmpty(dbToken)) {
                            kvService.put(tokenKey, token,
                                          TokenUtil.AUTH_TOKEN_EXPIRE);
                            TOKEN_CACHE.put(name, token,
                                            TokenUtil.AUTH_TOKEN_EXPIRE);
                            return tokenCall.apply(token);
                        }
                    }
                }
            }
            return call.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String getTokenKey(String name) {
        return "PD/TOKEN/" + name;
    }

}
