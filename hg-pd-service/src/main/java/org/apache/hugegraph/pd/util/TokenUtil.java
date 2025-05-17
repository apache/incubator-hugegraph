package org.apache.hugegraph.pd.util;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.auth.AuthConstant;
import org.apache.hugegraph.auth.TokenGenerator;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.config.Server;
import org.apache.hugegraph.util.StringEncoding;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;

/**
 * @author zhangyingjie
 * @date 2023/4/25
 **/
public class TokenUtil {

    public static final long AUTH_TOKEN_EXPIRE = 3600 * 24L * 1000;

    private TokenGenerator generator;
    private Map<String, Server> servers = new HashMap<>();

    public TokenUtil(String secretKey, List<Server> servers) {
        this.generator = new TokenGenerator(secretKey);
        for (Server server : servers) {
            this.servers.put(server.getServer(), server);
        }
    }

    public static void main(String[] args) {

        List<Server> servers1 = PDConfig.getDefaultServers();
        TokenUtil util = new TokenUtil("FXQXbJtbCLxODc6tGci732pkH1cyf8Qg", servers1);
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
        String token = util.getToken(servers1.get(0).getServer());
        System.out.println(token);
    }

    public String createToken(String userName) {
        Map<String, ?> payload = ImmutableMap.of(AuthConstant.TOKEN_USER_NAME, userName);
        byte[] bytes = generator.create(payload, AUTH_TOKEN_EXPIRE).getBytes(StandardCharsets.UTF_8);
        byte[] encode = Base64.getEncoder().encode(bytes);
        return new String(encode, Charsets.UTF_8);
    }

    public String getToken(String appName) {
        Server info = servers.get(appName);
        if (info != null) {
            return createToken(appName);
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

    public Server getInfo(String appName) {
        Server server = servers.get(appName);
        return server;
    }
}
