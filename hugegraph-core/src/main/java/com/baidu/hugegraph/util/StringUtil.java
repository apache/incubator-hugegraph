package com.baidu.hugegraph.util;

import java.util.Collection;

/**
 * Created by jishilei on 2017/3/22.
 */
public class StringUtil {

    public static String desc(String prefix, Collection<String> elems) {
        StringBuilder sb = new StringBuilder();
        for (String elem : elems) {
            sb.append("\"").append(elem).append("\",");
        }
        int endIdx = sb.lastIndexOf(",") > 0 ? sb.length() - 1 : sb.length();
        return String.format(".%s(%s)", prefix, sb.substring(0, endIdx));
    }

    public static void checkName(String name) {
        // TODO: perform is by regex
        E.checkNotNull(name, "name");
        E.checkArgument(!name.isEmpty(), "name can't be empty.");
        E.checkArgument(name.length() < 256,
                        "The length of name must less than 256 bytes.");
        E.checkArgument(name.substring(0, 1) != "_",
                        "The first letter of name can't be '_'.");
        E.checkArgument(!name.contains("\u0001"),
                        "name can't contain the character '\u0001'.");
        E.checkArgument(!name.contains("\u0002"),
                        "name can't contain the character '\u0002'.");
    }

}
