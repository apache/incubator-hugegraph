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

package org.apache.hugegraph.pd.util;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.apache.hugegraph.auth.AuthConstant;
import org.apache.hugegraph.auth.TokenGenerator;
import org.apache.hugegraph.util.StringEncoding;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;

public class TokenUtil {

    private TokenGenerator generator;
    public static final long AUTH_TOKEN_EXPIRE = 3600 * 24L * 1000;
    private static String[] storeInfo = {"store",
                                         "$2a$04$9ZGBULe2vc73DMj7r" +
                                         "/iBKeQB1SagtUXPrDbMmNswRkTwlWQURE/Jy",
                                         "E3UnnQa605go"};
    private static String[] serverInfo = {"hg",
                                          "$2a$04$i10KooNg6wLvIPVDh909n" +
                                          ".RBYlZ/4pJo978nFK86nrqQiGIKV4UGS",
                                          "qRyYhxVAWDb5"};
    private static String[] hubbleInfo = {"hubble",
                                          "$2a$04$pSGkohaywGgFrJLr6VOPm" +
                                          ".IK2WtOjlNLcZN8gct5uIKEDO1I61DGa",
                                          "iMjHnUl5Pprx"};
    private static String[] vermeer = {"vermeer",
                                       "$2a$04$N89qHe0v5jqNJKhQZHnTdOFSGmiNoiA2B2fdWpV2BwrtJK72dXYD.",
                                       "FqU8BOvTpteT"};
    private static Map<String, String[]> apps = new HashMap<>() {{
        put(storeInfo[0], storeInfo);
        put(serverInfo[0], serverInfo);
        put(hubbleInfo[0], hubbleInfo);
        put(vermeer[0], vermeer);
    }};

    public TokenUtil(String secretKey) {
        this.generator = new TokenGenerator(secretKey);
    }

    // public String getToken(String[] info) {
    //    Id id = new IdGenerator.UuidId(UUID.fromString(info[0]));
    //    Map<String, ?> payload = ImmutableMap.of(AuthConstant.TOKEN_USER_NAME,
    //                                             info[0],
    //                                             AuthConstant.TOKEN_USER_ID,
    //                                             id.asString());
    //    return generator.create(payload, AUTH_TOKEN_EXPIRE);
    // }
    public String getToken(String[] info) {
        Map<String, ?> payload = ImmutableMap.of(AuthConstant.TOKEN_USER_NAME,
                                                 info[0]);
        byte[] bytes =
                generator.create(payload, AUTH_TOKEN_EXPIRE).getBytes(StandardCharsets.UTF_8);
        byte[] encode = Base64.getEncoder().encode(bytes);
        return new String(encode, Charsets.UTF_8);
    }

    public String getToken(String appName) {
        String[] info = apps.get(appName);
        if (info != null) {
            return getToken(info);
        }
        return null;
    }

    public boolean verify(String token, String[] info) {
        byte[] decode = Base64.getDecoder().decode(token);
        String d = new String(decode, StandardCharsets.UTF_8);
        if (d.equals(info[1])) {
            return true;
        }
        return false;
    }

    public String[] getInfo(String appName) {
        return apps.get(appName);
    }

    public static void main(String[] args) {
        TokenUtil util = new TokenUtil("FXQXbJtbCLxODc6tGci732pkH1cyf8Qg");
        // String uniqueToken = util.getStoreToken();
        String x = StringEncoding.hashPassword("FqU8BOvTpteT");
        // String x = "$2a$04$i10KooNg6wLvIPVDh909n.RBYlZ/4pJo978nFK86nrqQiGIKV4UGS";
        System.out.println(x);
        // System.out.println(StringEncoding.checkPassword("qRyYhxVAWDb5", x));
        // $2a$04$9ZGBULe2vc73DMj7r/iBKeQB1SagtUXPrDbMmNswRkTwlWQURE/Jy "E3UnnQa605go"
        // $2a$04$i10KooNg6wLvIPVDh909n.RBYlZ/4pJo978nFK86nrqQiGIKV4UGS "qRyYhxVAWDb5"
        // $2a$04$pSGkohaywGgFrJLr6VOPm.IK2WtOjlNLcZN8gct5uIKEDO1I61DGa "iMjHnUl5Pprx"
        // eyJhbGciOiJIUzI1NiJ9
        // .eyJ1c2VyX25hbWUiOiJzdG9yZSIsInVzZXJfaWQiOiJhZWEwOTM1Ni0xZWJhLTQ1NjktODk0ZS1kYWIzZTRhYTYyM2MiLCJleHAiOjE2ODI1MDQ0MTd9.lDqbt3vZkE3X2IIK9A404BBlCFHBaEVsIycH0AIXKsw
        String token = util.getToken(serverInfo);
        System.out.println(token);
    }
}
