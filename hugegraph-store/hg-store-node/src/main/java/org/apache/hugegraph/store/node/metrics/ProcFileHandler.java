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
package org.apache.hugegraph.store.node.metrics;

import static java.util.Locale.ENGLISH;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import lombok.Getter;

class ProcFileHandler {

    // Cache duration in milliseconds
    private static final long CACHE_DURATION_MS = 100;
    // Singleton instances of ProcFileHandler
    private static final Map<String, ProcFileHandler> handlerInstances = new HashMap<>();
    private static final Object handlerLock = new Object();
    // Cached data
    private static final Map<Path, List<String>> cachedContent = new HashMap<>();
    private static final Object contentLock = new Object();
    // Base path for proc filesystem
    private static final Path PROC_BASE_PATH = Paths.get("/proc", "self");
    private final Path filePath;
    private final boolean isOSSupported;
    private long lastReadTimestamp = -1;

    // Private constructor to initialize the handler
    private ProcFileHandler(String entry) {
        this(PROC_BASE_PATH, entry, false);
    }

    // Package-private constructor for testing
    ProcFileHandler(Path base, String entry) {
        this(base, entry, true);
    }

    // Private constructor with an OS support flag
    private ProcFileHandler(Path base, String entry, boolean forceOSSupport) {
        Objects.requireNonNull(base);
        Objects.requireNonNull(entry);

        this.filePath = base.resolve(entry);
        this.isOSSupported = forceOSSupport ||
                             System.getProperty("os.name").toLowerCase(ENGLISH).startsWith("linux");
    }

    // Get an instance of ProcFileHandler
    static ProcFileHandler getInstance(String key) {
        Objects.requireNonNull(key);

        synchronized (handlerLock) {
            ProcFileHandler handler = handlerInstances.get(key);
            if (handler == null) {
                handler = new ProcFileHandler(key);
                handlerInstances.put(key, handler);
            }
            return handler;
        }
    }

    // Get the file path
    Path getFilePath() {
        return filePath;
    }

    // Read the proc file
    ReadResult readFile() throws IOException {
        return readFile(currentTimeMillis());
    }

    // Read the proc file with a specific time
    ReadResult readFile(long currentTimeMillis) throws IOException {
        synchronized (contentLock) {
            final Path key = getFilePath().getFileName();

            final ReadResult readResult;
            if (lastReadTimestamp == -1 ||
                lastReadTimestamp + CACHE_DURATION_MS < currentTimeMillis) {
                final List<String> lines = readFilePath(filePath);
                cacheContent(key, lines);
                lastReadTimestamp = currentTimeMillis();
                readResult = new ReadResult(lines, lastReadTimestamp);
            } else {
                readResult = new ReadResult(cachedContent.get(key), lastReadTimestamp);
            }
            return readResult;
        }
    }

    // Read the content of the path
    List<String> readFilePath(Path path) throws IOException {
        Objects.requireNonNull(path);

        if (!isOSSupported) {
            return Collections.emptyList();
        }
        return Files.readAllLines(path);
    }

    // Cache the result
    void cacheContent(Path key, List<String> lines) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(lines);

        cachedContent.put(key, lines);
    }

    // Get the current time in milliseconds
    long currentTimeMillis() {
        return System.currentTimeMillis();
    }

    // Result of reading the proc file
    @Getter
    static class ReadResult {

        private final List<String> lines;
        private final long readTime;

        ReadResult(List<String> lines, long readTime) {
            this.lines = Objects.requireNonNull(lines);
            this.readTime = readTime;
        }
    }
}
