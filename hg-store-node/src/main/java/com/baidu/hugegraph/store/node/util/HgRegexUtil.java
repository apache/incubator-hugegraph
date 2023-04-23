package com.baidu.hugegraph.store.node.util;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author lynn.bond@hotmail.com created on 2022/03/07
 */
public class HgRegexUtil {

    public static String getGroupValue(String regex, String source, int groupId) {
        if (regex == null || "".equals(regex) || source == null || "".equals(source)) return null;

        String value = "";

        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(source);

        while (m.find()) {
            for (int i = 0; i <= m.groupCount(); i++) {
                if (i == groupId) {
                    value = m.group(i);
                }
            }
        }
        return value;
    }

    public static List<String> toGroupValues(String regex, String source) {
        if (regex == null || "".equals(regex) || source == null || "".equals(source)) return null;

        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(source);

        List<String> list = null;

        while (m.find()) {
            list = new ArrayList<>(m.groupCount());
            for (int i = 0; i <= m.groupCount(); i++) {
                list.add(m.group(i));
            }
        }

        return list;
    }

    public static List<String> getMatchList(String regex, String source) {
        if (regex == null || "".equals(regex) || source == null || "".equals(source)) return null;
        Pattern p = Pattern.compile(regex, Pattern.MULTILINE);
        Matcher m = p.matcher(source);
        List<String> list = new ArrayList<String>();
        while (m.find()) {
            list.add(m.group(0));
        }
        return list.isEmpty() ? null : list;
    }


    public static void main(String[] args) {
        List<String> res = toGroupValues("(replicator)(.+?:\\d+)(.*)", "replicator_10.14.139.10:8081_append_entries_times");
        if (res != null) {
            res.stream().forEach(e -> System.out.println(e));
        }
    }

}
