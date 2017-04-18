package com.baidu.hugegraph.util;

import java.util.Set;

import com.google.common.base.Preconditions;

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

    public static void verifyName(String name) {
        Preconditions.checkNotNull(name, "name can not be null.");
        Preconditions.checkNotNull(!name.isEmpty(), "name can not be empty.");
        Preconditions.checkArgument(name.length() < 256, "the length of name must less than 256.");
        Preconditions.checkArgument(name.substring(0, 1) != "_", "The first letter of name can not be '_'.");
        Preconditions.checkArgument(!name.contains("\u0001"), "name can not contain the character '\u0001'.");
        Preconditions.checkArgument(!name.contains("\u0002"), "name can not contain the character '\u0002'.");
    }

}
