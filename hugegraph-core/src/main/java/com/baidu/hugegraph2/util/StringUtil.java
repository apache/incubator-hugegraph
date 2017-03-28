package com.baidu.hugegraph2.util;

import java.util.Set;

/**
 * Created by jishilei on 2017/3/22.
 */
public class StringUtil {

    public static String descSchema(String prefix, Set<String> elems) {
        String desc = "";
        if (elems != null) {
            desc += ".";
            desc += prefix;
            desc += "(";
            for (String elem : elems) {
                desc += "\"";
                desc += elem;
                desc += "\",";
            }
            int endIdx = desc.lastIndexOf(",") > 0 ? desc.length() - 1 : desc.length();
            desc = desc.substring(0, endIdx) + ")";
        }
        return desc;
    }
}
