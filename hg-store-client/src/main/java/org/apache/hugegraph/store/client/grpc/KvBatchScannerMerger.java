/*
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

package org.apache.hugegraph.store.client.grpc;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.hugegraph.store.HgKvEntry;
import org.apache.hugegraph.store.HgKvIterator;
import org.apache.hugegraph.store.HgKvOrderedIterator;
import org.apache.hugegraph.store.HgPageSize;
import org.apache.hugegraph.store.client.util.PropertyUtil;

import lombok.extern.slf4j.Slf4j;

/**
 * 批量查询结果归并，阻塞队列工作模式
 * 对请求任务的拆分,创建多个请求队列
 */
@Slf4j
public class KvBatchScannerMerger implements KvCloseableIterator<HgKvIterator<HgKvEntry>>,
                                             HgPageSize {

    static int maxWaitCount = PropertyUtil.getInt("net.kv.scanner.wait.timeout", 60);
    protected final BlockingQueue<Supplier<HgKvIterator<HgKvEntry>>> queue =
            new LinkedBlockingQueue<>();
    private final KvBatchScanner.TaskSplitter taskSplitter;
    private final List<KvBatchScanner> scanners = new CopyOnWriteArrayList<>();
    private Supplier<HgKvIterator<HgKvEntry>> current = null;

    public KvBatchScannerMerger(KvBatchScanner.TaskSplitter splitter) {
        this.taskSplitter = splitter;
        splitter.setNotifier(this);
    }

    public void startTask() {
        taskSplitter.splitTask();
    }

    public void dataArrived(KvBatchScanner scanner, Supplier<HgKvIterator<HgKvEntry>> supplier)
            throws InterruptedException {
        queue.put(supplier);
    }

    @Override
    public boolean hasNext() {
        int waitTime = 0;
        while (current == null) {
            try {
                // 队列有数据，还有活跃的查询器，任务未分配完
                if (queue.size() != 0 || scanners.size() > 0 || !taskSplitter.isFinished()) {
                    current = queue.poll(1, TimeUnit.SECONDS);  //定期检查client是否被关闭了
                } else {
                    break;
                }
                if (current == null) {
                    // 超时重试
                    sendTimeout();
                    if (++waitTime > maxWaitCount) {
                        log.error(
                                "KvBatchScanner wait data timeout {}, closeables is {}, task is {}",
                                waitTime, scanners.size(), taskSplitter.isFinished());
                        break;
                    }
                }
            } catch (InterruptedException e) {
                log.error("hasNext interrupted {}", e);
                throw new RuntimeException(e.getMessage(), e);
            }
        }
        return current != null && current != KvBatchScanner.NO_DATA;
    }


    @Override
    public HgKvIterator<HgKvEntry> next() {
        HgKvIterator<HgKvEntry> iterator = null;
        if (current != null) {
            iterator = current.get();
        }
        current = null;
        return iterator;
    }

    @Override
    public void close() {
        taskSplitter.close();
        scanners.forEach(c -> c.close());
    }

    private void sendTimeout() {
        scanners.forEach(v -> {
            v.sendResponse();
        });
    }

    @Override
    public long getPageSize() {
        return 0;
    }

    public void registerScanner(KvBatchScanner closeable) {
        this.scanners.add(closeable);
    }

    /**
     * 返回值<0表示任务结束
     *
     * @param closeable
     * @return
     */
    public int unregisterScanner(KvBatchScanner closeable) {
        this.scanners.remove(closeable);
        try {
            taskSplitter.splitTask();
        } catch (Exception e) {
            log.error("exception ", e);
        }
        return taskSplitter.isFinished() && this.scanners.size() == 0 ?
               -1 : this.scanners.size();
    }

    public int getScannerCount() {
        return this.scanners.size();
    }

    /**
     * 组装一个Scanner的多个有序迭代器为一个迭代器
     */
    static class ScannerDataQueue {
        private BlockingQueue<Supplier<HgKvIterator<HgKvEntry>>> queue;
        private HgKvOrderedIterator<HgKvEntry> iterator = null;
        private int currentSN = 0;
        private HgKvEntry entry;

        public ScannerDataQueue() {
            queue = new LinkedBlockingQueue<>();
        }

        public int sn() {
            return currentSN;
        }

        public void add(Supplier<HgKvIterator<HgKvEntry>> supplier) {
            if (queue != null) {
                queue.add(supplier);
            }
        }

        /**
         * 迭代器是否有效，如果没有数据，等待数据到达
         *
         * @return
         */
        public boolean hasNext() {
            while (entry == null && queue != null) {
                try {
                    int waitTime = 0;
                    Supplier<HgKvIterator<HgKvEntry>> current;
                    current = queue.poll(1, TimeUnit.SECONDS);  //定期检查client是否被关闭了
                    if (current == null) {
                        if (++waitTime > maxWaitCount) {
                            break;
                        }
                    } else if (current == KvBatchScanner.NO_DATA) {
                        queue = null;
                        break;
                    } else {
                        iterator = (HgKvOrderedIterator<HgKvEntry>) current.get();
                        if (iterator != null && iterator.hasNext()) {
                            moveNext();
                        } else {
                            iterator = null;
                        }
                    }
                } catch (InterruptedException e) {
                    log.error("hasNext interrupted {}", e);
                    throw new RuntimeException(e.getMessage(), e);
                }
            }
            return entry != null;
        }

        public HgKvEntry next() {
            HgKvEntry current = entry;
            moveNext();
            return current;
        }

        private void moveNext() {
            if (iterator.hasNext()) {
                entry = iterator.next();
                currentSN = (int) iterator.getSequence();
            } else {
                entry = null;
                iterator = null;
            }
        }
    }

    /**
     * 对多个Scanner返回结果进行归并排序
     */
    static class SortedScannerMerger extends KvBatchScannerMerger {
        // 每一个流对应一个接收队列
        private final Map<KvBatchScanner, ScannerDataQueue> scannerQueues =
                new ConcurrentHashMap<>();

        public SortedScannerMerger(KvBatchScanner.TaskSplitter splitter) {
            super(splitter);
            queue.add(() -> {
                // 对store返回结果进行归并排序
                return new HgKvIterator<>() {
                    private ScannerDataQueue iterator;
                    private int currentSN = 0;
                    private HgKvEntry entry;

                    @Override
                    public byte[] key() {
                        return entry.key();
                    }

                    @Override
                    public byte[] value() {
                        return entry.value();
                    }

                    @Override
                    public void close() {

                    }

                    @Override
                    public byte[] position() {
                        return new byte[0];
                    }

                    @Override
                    public void seek(byte[] position) {
                        throw new RuntimeException("not implemented");
                    }

                    @Override
                    public boolean hasNext() {
                        //
                        if (iterator == null || !iterator.hasNext() || currentSN != iterator.sn()) {
                            iterator = selectIterator();
                        }

                        if (iterator != null) {
                            currentSN = iterator.sn();
                        }
                        return iterator != null;
                    }

                    @Override
                    public HgKvEntry next() {
                        entry = iterator.next();
                        return entry;
                    }
                };
            });
        }

        /**
         * 从多个Scanner中挑选一个sn最小的迭代器
         * 如果Scanner没有数据，等待数据到达。
         *
         * @return
         */
        private ScannerDataQueue selectIterator() {
            int sn = Integer.MAX_VALUE;
            ScannerDataQueue current = null;
            while (current == null && !scannerQueues.isEmpty()) {
                Iterator<KvBatchScanner> itr = scannerQueues.keySet().iterator();
                while (itr.hasNext()) {
                    KvBatchScanner key = itr.next();
                    ScannerDataQueue kvItr = scannerQueues.get(key);
                    if (!kvItr.hasNext()) {
                        scannerQueues.remove(key);
                        continue;
                    }
                    if (kvItr.sn() <= sn) {
                        sn = kvItr.sn();
                        current = kvItr;
                    }
                }
            }
            return current;
        }

        @Override
        public void registerScanner(KvBatchScanner scanner) {
            super.registerScanner(scanner);
            scannerQueues.putIfAbsent(scanner, new ScannerDataQueue());
        }

        @Override
        public int unregisterScanner(KvBatchScanner scanner) {
            dataArrived(scanner, KvBatchScanner.NO_DATA);
            return super.unregisterScanner(scanner);
        }

        @Override
        public void dataArrived(KvBatchScanner scanner,
                                Supplier<HgKvIterator<HgKvEntry>> supplier) {
            scannerQueues.putIfAbsent(scanner, new ScannerDataQueue());
            scannerQueues.get(scanner).add(supplier);
        }
    }
}
