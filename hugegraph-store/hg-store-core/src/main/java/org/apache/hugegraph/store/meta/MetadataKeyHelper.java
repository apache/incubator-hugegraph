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

package org.apache.hugegraph.store.meta;

import java.nio.charset.StandardCharsets;

public class MetadataKeyHelper {

    private static final char DELIMITER = '/';
    private static final String HUGEGRAPH = "HUGEGRAPH";
    private static final String STORE = "STORE";
    private static final String PARTITION = "PARTITION";
    private static final String TASK = "TASK";
    private static final String ASYNC_TASK = "A_TASK";
    private static final String INSTRUCTION_TASK = "INS_TASK";
    private static final String TASK_DONE = "TASK_DONE";
    private static final String GRAPH = "GRAPH";
    private static final String PARTITION_STORE = "PARTITION_STORE";
    private static final String PARTITION_RAFT = "PARTITION_Raft";
    private static final String DELETED_FILE = "DELETED_FILE";
    private static final String SHARD_GROUP = "SHARDGROUP";

    private static final String CID_PREFIX = "CID";
    private static final String CID_SLOT_PREFIX = "CID_SLOT";
    private static final String GRAPH_ID_PREFIX = "GRAPH_ID";

    public static byte[] getPartitionKey(String graph, Integer partId) {
        // HUGEGRAPH/Partition/{graph}/partId
        String key = StringBuilderHelper.get()
                                        .append(HUGEGRAPH).append(DELIMITER)
                                        .append(PARTITION).append(DELIMITER)
                                        .append(graph).append(DELIMITER)
                                        .append(partId)
                                        .toString();
        return key.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] getPartitionPrefixKey(String graph) {
        // HUGEGRAPH/Partition/{graph}/
        String key = StringBuilderHelper.get()
                                        .append(HUGEGRAPH).append(DELIMITER)
                                        .append(PARTITION).append(DELIMITER)
                                        .append(graph).append(DELIMITER)
                                        .toString();
        return key.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * 查询分区内的所有partition prefix， 不包含 graph name
     *
     * @return
     */
    public static byte[] getPartitionPrefixKey() {
        // HUGEGRAPH/Partition/
        String key = StringBuilderHelper.get()
                                        .append(HUGEGRAPH).append(DELIMITER)
                                        .append(PARTITION).append(DELIMITER)
                                        .toString();
        return key.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] getShardGroupKey(int partitionId) {
        // HUGEGRAPH/SHARDGROUP/{partition_id}
        String key = StringBuilderHelper.get()
                                        .append(HUGEGRAPH).append(DELIMITER)
                                        .append(SHARD_GROUP).append(DELIMITER)
                                        .append(partitionId)
                                        .toString();
        return key.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] getGraphKey(String graph) {
        // HUGEGRAPH/Graph/{graph}
        String key = StringBuilderHelper.get()
                                        .append(HUGEGRAPH).append(DELIMITER)
                                        .append(GRAPH).append(DELIMITER)
                                        .append(graph)
                                        .toString();
        return key.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] getGraphKeyPrefix() {
        // HUGEGRAPH/Graph/{graph}
        String key = StringBuilderHelper.get()
                                        .append(HUGEGRAPH).append(DELIMITER)
                                        .append(GRAPH).append(DELIMITER)
                                        .toString();
        return key.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] getStoreKey() {
        // HUGEGRAPH/STORE/
        String key = StringBuilderHelper.get()
                                        .append(HUGEGRAPH).append(DELIMITER)
                                        .append(STORE)
                                        .toString();
        return key.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] getTaskKey(int partId, String type, long taskId) {
        // HUGEGRAPH/TASK/
        String key = StringBuilderHelper.get()
                                        .append(HUGEGRAPH).append(DELIMITER)
                                        .append(TASK).append(DELIMITER)
                                        .append(partId).append(DELIMITER)
                                        .append(type).append(DELIMITER)
                                        .append(String.format("%016x", taskId))
                                        .toString();
        return key.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] getInstructionIdKey(long taskId) {
        String key = StringBuilderHelper.get()
                                        .append(HUGEGRAPH).append(DELIMITER)
                                        .append(INSTRUCTION_TASK).append(DELIMITER)
                                        .append(taskId).append(DELIMITER)
                                        .toString();
        return key.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] getAsyncTaskKey(int partId, String graphName, String taskId) {
        // HUGEGRAPH/A_TASK/ part id / graphName / task id
        String key = StringBuilderHelper.get()
                                        .append(HUGEGRAPH).append(DELIMITER)
                                        .append(ASYNC_TASK).append(DELIMITER)
                                        .append(partId).append(DELIMITER)
                                        .append(graphName).append(DELIMITER)
                                        .append(taskId)
                                        .toString();
        return key.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] getAsyncTaskPrefix(int partId, String graphName) {
        // HUGEGRAPH/A_TASK/ part id / graphName / task id
        String key = StringBuilderHelper.get()
                                        .append(HUGEGRAPH).append(DELIMITER)
                                        .append(ASYNC_TASK).append(DELIMITER)
                                        .append(partId).append(DELIMITER)
                                        .append(graphName).append(DELIMITER)
                                        .toString();
        return key.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] getTaskPrefix(int partId, String type) {
        // HUGEGRAPH/TASK/
        String key = StringBuilderHelper.get()
                                        .append(HUGEGRAPH).append(DELIMITER)
                                        .append(TASK).append(DELIMITER)
                                        .append(partId).append(DELIMITER)
                                        .append(type).append(DELIMITER)
                                        .toString();
        return key.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] getTaskPrefix(int partId) {
        // HUGEGRAPH/TASK/
        String key = StringBuilderHelper.get()
                                        .append(HUGEGRAPH).append(DELIMITER)
                                        .append(TASK).append(DELIMITER)
                                        .append(partId).append(DELIMITER)
                                        .toString();
        return key.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] getTaskPrefix() {
        // HUGEGRAPH/TASK/
        String key = StringBuilderHelper.get()
                                        .append(HUGEGRAPH).append(DELIMITER)
                                        .append(TASK).append(DELIMITER)
                                        .toString();
        return key.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] getDoneTaskKey(long taskId) {
        // HUGEGRAPH/TASK/
        String key = StringBuilderHelper.get()
                                        .append(HUGEGRAPH).append(DELIMITER)
                                        .append(TASK_DONE).append(DELIMITER)
                                        .append(String.format("%016x", taskId))
                                        .toString();
        return key.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] getPartitionStoreKey(int partId) {
        // HUGEGRAPH/TASK/
        String key = StringBuilderHelper.get()
                                        .append(HUGEGRAPH).append(DELIMITER)
                                        .append(PARTITION_STORE).append(DELIMITER)
                                        .append(partId)
                                        .toString();
        return key.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] getPartitionStorePrefix() {
        // HUGEGRAPH/TASK/
        String key = StringBuilderHelper.get()
                                        .append(HUGEGRAPH).append(DELIMITER)
                                        .append(PARTITION_STORE).append(DELIMITER)
                                        .toString();
        return key.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] getPartitionRaftKey(int partId) {
        // HUGEGRAPH/TASK/
        String key = StringBuilderHelper.get()
                                        .append(HUGEGRAPH).append(DELIMITER)
                                        .append(PARTITION_RAFT).append(DELIMITER)
                                        .append(partId)
                                        .toString();
        return key.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] getPartitionRaftPrefix() {
        // HUGEGRAPH/TASK/
        String key = StringBuilderHelper.get()
                                        .append(HUGEGRAPH).append(DELIMITER)
                                        .append(PARTITION_RAFT).append(DELIMITER)
                                        .toString();
        return key.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] getDeletedFileKey(String filePath) {
        // HUGEGRAPH/TASK/
        String key = StringBuilderHelper.get()
                                        .append(HUGEGRAPH).append(DELIMITER)
                                        .append(DELETED_FILE).append(DELIMITER)
                                        .append(filePath)
                                        .toString();
        return key.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] getDeletedFilePrefix() {
        // HUGEGRAPH/TASK/
        String key = StringBuilderHelper.get()
                                        .append(HUGEGRAPH).append(DELIMITER)
                                        .append(DELETED_FILE).append(DELIMITER)
                                        .toString();
        return key.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] getCidKey(String name) {
        // HUGEGRAPH/CID/
        String key = StringBuilderHelper.get()
                                        .append(HUGEGRAPH).append(DELIMITER)
                                        .append(CID_PREFIX).append(DELIMITER)
                                        .append(name)
                                        .toString();
        return key.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] getCidSlotKeyPrefix(String name) {
        // HUGEGRAPH/CID_SLOT/
        String key = StringBuilderHelper.get()
                                        .append(HUGEGRAPH).append(DELIMITER)
                                        .append(CID_SLOT_PREFIX).append(DELIMITER)
                                        .append(name).append(DELIMITER)
                                        .toString();
        return key.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] getGraphIDKey(String graph) {
        // HUGEGRAPH/Graph/{graph}
        String key = StringBuilderHelper.get()
                                        .append(HUGEGRAPH).append(DELIMITER)
                                        .append(GRAPH_ID_PREFIX).append(DELIMITER)
                                        .append(graph)
                                        .toString();
        return key.getBytes(StandardCharsets.UTF_8);
    }

    static class StringBuilderHelper {

        private static final int DISCARD_LIMIT = 1024 << 3;     // 8k

        private static final ThreadLocal<StringBuilderHolder> holderThreadLocal = ThreadLocal
                .withInitial(StringBuilderHolder::new);

        public static StringBuilder get() {
            final StringBuilderHolder holder = holderThreadLocal.get();
            return holder.getStringBuilder();
        }

        public static void truncate() {
            final StringBuilderHolder holder = holderThreadLocal.get();
            holder.truncate();
        }

        private static class StringBuilderHolder {

            private final StringBuilder buf = new StringBuilder();

            private StringBuilder getStringBuilder() {
                truncate();
                return buf;
            }

            private void truncate() {
                buf.setLength(0);
            }
        }
    }
}
