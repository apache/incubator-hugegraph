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

package org.apache.hugegraph.store.node.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * created on 2022/03/07
 */
public class HgRegexUtil {

    public static String getGroupValue(String regex, String source, int groupId) {
        if (regex == null || "".equals(regex) || source == null || "".equals(source)) {
            return null;
        }

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
        if (regex == null || "".equals(regex) || source == null || "".equals(source)) {
            return null;
        }

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
        if (regex == null || "".equals(regex) || source == null || "".equals(source)) {
            return null;
        }
        Pattern p = Pattern.compile(regex, Pattern.MULTILINE);
        Matcher m = p.matcher(source);
        List<String> list = new ArrayList<>();
        while (m.find()) {
            list.add(m.group(0));
        }
        return list.isEmpty() ? null : list;
    }

    public static void main(String[] args) {
        List<String> res = toGroupValues("(replicator)(.+?:\\d+)(.*)",
                                         "replicator_10.14.139.10:8081_append_entries_times");
        if (res != null) {
            res.stream().forEach(e -> System.out.println(e));
        }
    }

}
