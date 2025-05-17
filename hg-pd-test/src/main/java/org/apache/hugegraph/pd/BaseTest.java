package org.apache.hugegraph.pd;

import org.apache.hugegraph.pd.client.PDConfig;

/**
 * @author zhangyingjie
 * @date 2023/10/11
 **/
public class BaseTest {

    protected static String pdGrpcAddr = "10.108.17.32:8686";
    protected static String pdRestAddr = "http://10.108.17.32:8620";
    protected static String user = "store";
    protected static String pwd = "$2a$04$9ZGBULe2vc73DMj7r/iBKeQB1SagtUXPrDbMmNswRkTwlWQURE/Jy";
    protected static String key = "Authorization";
    protected static String value = "Basic c3RvcmU6YWRtaW4=";

    protected PDConfig getPdConfig() {
        return PDConfig.of(pdGrpcAddr).setAuthority(user, pwd);
    }
}
