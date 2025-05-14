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

import static org.apache.hugegraph.store.node.metrics.ProcFileHandler.ReadResult;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class ProcfsRecord {

    private static final Logger logger = LoggerFactory.getLogger(ProcfsRecord.class);

    private final Object syncLock = new Object();

    private final ProcFileHandler fileReader;

    private long lastProcessedTime = -1;

    protected ProcfsRecord(ProcFileHandler fileReader) {
        this.fileReader = Objects.requireNonNull(fileReader);
    }

    protected final void gatherData() {
        synchronized (syncLock) {
            try {
                final ReadResult readResult = fileReader.readFile();
                if (readResult != null &&
                    (lastProcessedTime == -1 || lastProcessedTime != readResult.getReadTime())) {
                    clear();
                    process(readResult.getLines());
                    lastProcessedTime = readResult.getReadTime();
                }
            } catch (IOException e) {
                clear();
                logger.warn("Failed reading '" + fileReader.getFilePath() + "'!", e);
            }
        }
    }

    protected abstract void clear();

    protected abstract void process(Collection<String> lines);

}
