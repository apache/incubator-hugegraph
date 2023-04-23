package com.baidu.hugegraph.store.util;

import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.io.Reader;

@Slf4j
public class Version {

    private static String version = "";
    /**
     * 软件版本号
     * @return
     */
    public static String getVersion() {
        if (version.isEmpty()) {
            try (InputStream is = Version.class.getResourceAsStream("/version.txt")) {
                byte[] buf = new byte[64];
                int len = is.read(buf);
                version = new String(buf, 0, len);
            } catch (Exception e) {
                log.error("Version.getVersion exception {}", e);
            }
        }
        return version;
    }

    /**
     * 存储格式版本号
     * @return
     */
    public static int getDataFmtVersion(){
        return 1;
    }
}
