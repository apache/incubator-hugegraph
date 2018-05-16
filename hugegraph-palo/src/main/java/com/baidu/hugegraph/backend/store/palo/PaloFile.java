/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.backend.store.palo;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.util.E;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;

public class PaloFile extends File {

    private static final long serialVersionUID = -1918775445693598353L;

    public PaloFile(String dir, String subDir, String fileName) {
        this(Paths.get(dir, subDir, fileName).toString());
    }

    public PaloFile(String path, int id, int part) {
        this(Paths.get(path, formatFileName(id, part)).toString());
    }

    public PaloFile(String path) {
        super(path);
    }

    public String table() {
        return this.getParentFile().getName();
    }

    public int sessionId() {
        String[] parts = this.getName().split("-");
        E.checkState(parts.length == 2,
                     "Invalid file name format '%s' for palo temp file, " +
                     "the legal format is session{m}-part{n}", this.getName());
        return Integer.parseInt(parts[0].substring("session".length()));
    }

    public int sessionPart() {
        String[] parts = this.getName().split("-");
        E.checkState(parts.length == 2,
                     "Invalid file name format '%s' for palo temp file, " +
                     "the legal format is session{m}-part{n}", this.getName());
        return Integer.parseInt(parts[1].substring("part".length()));
    }

    public int writeLines(Collection<String> lines) {
        try {
            FileUtils.writeLines(this, Charsets.UTF_8.name(), lines, true);
        } catch (IOException e) {
            throw new BackendException(e);
        }
        return lines.size();
    }

    public String readAsString() {
        try {
            return FileUtils.readFileToString(this);
        } catch (IOException e) {
            throw new BackendException(e);
        }
    }

    public void forceDelete() {
        if (this.exists()) {
            try {
                FileUtils.forceDelete(this);
            } catch (IOException e) {
                throw new BackendException(e);
            }
        }
    }

    public static void clearDir(String tempDir) {
        File file = FileUtils.getFile(tempDir);
        if (!file.exists()) {
            return;
        }
        try {
            FileUtils.forceDelete(file);
        } catch (IOException e) {
            throw new BackendException(e);
        }
    }

    public static List<PaloFile> scan(String path, List<String> tableDirs) {
        File directory = FileUtils.getFile(path);
        if (!directory.exists()) {
            return ImmutableList.of();
        }

        File[] subDirs = directory.listFiles((dir, name) -> {
            return tableDirs.contains(name);
        });
        if (subDirs == null || subDirs.length == 0) {
            return ImmutableList.of();
        }

        List<PaloFile> paloFiles = new ArrayList<>(subDirs.length);
        for (File subDir : subDirs) {
            String[] fileNames = subDir.list();
            if (fileNames == null) {
                continue;
            }
            for (String fileName : fileNames) {
                paloFiles.add(new PaloFile(path, subDir.getName(), fileName));
            }
        }

        /*
         * Sort palo file by updated time in asc order,
         * let old files to be processed in priority
         */
        paloFiles.sort((file1, file2) -> {
            return (int) (file1.lastModified() - file2.lastModified());
        });
        return paloFiles;
    }

    private static String formatFileName(int id, int part) {
        return String.format("session%s-part%s", id, part);
    }

    private static int[] parseFileName(String fileName) {
        String[] nameParts = fileName.split("-");
        E.checkArgument(nameParts.length == 2,
                        "Invalid file name format '%s' for palo temp file, " +
                        "the legal format is session{m}-part{n}", fileName);
        int[] rs = new int[2];
        rs[0] = Integer.parseInt(nameParts[0].substring("session".length()));
        rs[1] = Integer.parseInt(nameParts[1].substring("part".length()));
        return rs;
    }

    public static long limitSize(HugeConfig config) {
        long limitSize = config.get(PaloOptions.PALO_FILE_LIMIT_SIZE);
        return limitSize * 1024 * 1024;
    }

    public static Set<Integer> scanSessionIds(HugeConfig config,
                                              List<String> tableDirs) {
        Set<Integer> sessionIds = new HashSet<>();

        String path = config.get(PaloOptions.PALO_TEMP_DIR);
        File pathDir = Paths.get(path).toFile();
        if (!pathDir.exists()) {
            return sessionIds;
        }
        for (String table : tableDirs) {
            File tableDir = Paths.get(path, table).toFile();
            if (!tableDir.exists()) {
                continue;
            }
            String[] fileNames = tableDir.list();
            if (fileNames == null || fileNames.length == 0) {
                continue;
            }
            for (String fileName : fileNames) {
                int[] parts = PaloFile.parseFileName(fileName);
                int sessionId = parts[0];
                sessionIds.add(sessionId);
            }
        }
        return sessionIds;
    }
}
