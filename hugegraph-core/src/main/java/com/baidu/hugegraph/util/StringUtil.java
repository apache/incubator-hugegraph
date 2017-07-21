package com.baidu.hugegraph.util;

import java.util.Collection;

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
        E.checkNotNull(name, "name");
        E.checkArgument(!name.isEmpty(),
                        "The name can't be empty.");
        E.checkArgument(name.length() < 256,
                        "The length of name must less than 256 bytes.");
        E.checkArgument(!name.startsWith("_"),
                        "The name can't be started with'_'.");

        final char[] filters = {'#', '>', ':', '!'};
        for (char c : filters) {
            E.checkArgument(name.indexOf(c) == -1,
                            "The name can't contain character '%s'.", c);
        }
    }

    public static String escape(char splitor, char escape, String... values) {
        StringBuilder escaped = new StringBuilder((values.length + 1) * 16);
        // Do escape for every item in values
        for (String value : values) {
            if (escaped.length() > 0) {
                escaped.append(splitor);
            }

            if (value.indexOf(splitor) == -1) {
                escaped.append(value);
                continue;
            }
            // Do escape for current item
            for (int i = 0; i < value.length(); i++) {
                char ch = value.charAt(i);
                if (ch == splitor) {
                    escaped.append(escape);
                }
                escaped.append(ch);
            }
        }
        return escaped.toString();
    }

    public static String[] unescape(String id, String splitor, String escape) {
        /*
         * NOTE: The `splitor`/`escape` may be special characters in regular
         * expressions, but this is a frequently called method, for faster
         * execution, we forbid the use of special characters as delimiter
         * or escape sign.
         */
        String[] parts = id.split("(?<!" + escape + ")" + splitor);
        for (int i = 0; i < parts.length; i++) {
            parts[i] = parts[i].replaceAll(escape + splitor, splitor);
        }
        return parts;
    }
}
