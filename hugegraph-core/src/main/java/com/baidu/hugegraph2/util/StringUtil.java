package com.baidu.hugegraph2.util;

/**
 * Created by jishilei on 2017/3/22.
 */
public class StringUtil {
    public static String captureName(String name) {
        char[] cs = name.toCharArray();
        cs[0] -= 32;
        return String.valueOf(cs);

    }
}
