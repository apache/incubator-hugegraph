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

package org.apache.hugegraph.store.cli.loader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.store.HgKvEntry;
import org.apache.hugegraph.store.HgKvIterator;
import org.apache.hugegraph.store.HgOwnerKey;
import org.apache.hugegraph.store.HgScanQuery;
import org.apache.hugegraph.store.HgStoreClient;
import org.apache.hugegraph.store.HgStoreSession;
import org.apache.hugegraph.store.cli.util.HgCliUtil;
import org.apache.hugegraph.store.client.grpc.KvCloseableIterator;
import org.apache.hugegraph.store.client.util.MetricX;

import lombok.extern.slf4j.Slf4j;

/**
 * 使用pd，支持raft
 * 读取文件并多线程进行入库
 */
@Slf4j
public class HgThread2DB {

    /*正在进行和在排队的任务的总数*/
    private static final AtomicInteger taskTotal = new AtomicInteger(0);
    private static final AtomicInteger queryTaskTotal = new AtomicInteger(0);
    private static final AtomicLong insertDataCount = new AtomicLong();
    private static final AtomicLong queryCount = new AtomicLong();
    private static final AtomicLong totalQueryCount = new AtomicLong();
    private static final AtomicLong longId = new AtomicLong();
    private static final CountDownLatch countDownLatch = null;
    private static PDClient pdClient;
    private static ThreadPoolExecutor threadPool = null;
    private static ThreadPoolExecutor queryThreadPool = null;
    private static int limitScanBatchCount = 100;
    private static ArrayBlockingQueue listQueue = null;
    private final HgStoreClient storeClient;
    public String graphName = "hugegraphtest";
    volatile long startTime = System.currentTimeMillis();

    public HgThread2DB(String pdAddr) {
        int threadCount = Runtime.getRuntime().availableProcessors();

        listQueue = new ArrayBlockingQueue<List<HgOwnerKey>>(100000000);
        queryThreadPool = new ThreadPoolExecutor(500, 1000,
                                                 200, TimeUnit.SECONDS,
                                                 new ArrayBlockingQueue<>(1000));
        threadPool = new ThreadPoolExecutor(threadCount * 2, threadCount * 3,
                                            200, TimeUnit.SECONDS,
                                            new ArrayBlockingQueue<>(threadCount + 100));
        storeClient = HgStoreClient.create(PDConfig.of(pdAddr)
                                                   .setEnableCache(true));
        pdClient = storeClient.getPdClient();
    }

    public void setGraphName(String graphName) {
        this.graphName = graphName;
        log.info("setGraphName {}", graphName);
    }

    public boolean singlePut(String tableName
            , List<String> keys) throws InterruptedException {
        HgStoreSession session = storeClient.openSession(graphName);
        session.beginTx();

        keys.forEach((strKey) -> {
            insertDataCount.getAndIncrement();
            int j = strKey.indexOf("\t");
//            byte[] key = HgCliUtil.toBytes(strKey.substring(0, j));
            HgOwnerKey hgKey = HgCliUtil.toOwnerKey(strKey.substring(0, j), strKey);
            byte[] value = HgCliUtil.toBytes(strKey.substring(j + 1));
            session.put(tableName, hgKey, value);

        });
        if (insertDataCount.get() > 10000000) {
            synchronized (insertDataCount) {
                long count = insertDataCount.get();
                insertDataCount.set(0);
                if (count > 10000000) {
                    log.info("count : " + count + " qps : " +
                             count * 1000 / (System.currentTimeMillis() - startTime)
                             + " threadCount : " + taskTotal);
                    startTime = System.currentTimeMillis();
                }
            }
        }
        if (!keys.isEmpty()) {
            if (session.isTx()) {
                session.commit();
            } else {
                session.rollback();
            }
        }

        return true;
    }

    public boolean singlePut(String tableName) throws InterruptedException {
        HgStoreSession session = storeClient.openSession(graphName);
        session.beginTx();

        int maxlist = 100;

        for (int y = 0; y < maxlist; y++) {
            insertDataCount.getAndIncrement();
            String strLine = getLong() + getLong() + getLong() + getLong();
            HgOwnerKey hgKey = HgCliUtil.toOwnerKey(strLine, strLine);
            byte[] value = HgCliUtil.toBytes(strLine);
            session.put(tableName, hgKey, value);
        }

        if (insertDataCount.get() > 10000000) {
            synchronized (insertDataCount) {
                long count = insertDataCount.get();
                insertDataCount.set(0);
                if (count > 10000000) {
                    log.info("count : " + count + " qps : " +
                             count * 1000 / (System.currentTimeMillis() - startTime)
                             + " threadCount : " + taskTotal);
                    startTime = System.currentTimeMillis();
                }
            }
        }

        if (session.isTx()) {
            session.commit();
        } else {
            session.rollback();
        }

        return true;
    }

    public boolean testOrder(String input) {
        String tableName = "hugegraph02";
        HgStoreSession session = storeClient.openSession(graphName);
        session.beginTx();
        int loop = Integer.parseInt(input);
        if (loop == 0) {
            loop = 2000;
        }
        for (int i = 0; i < loop; i++) {
            long startTime = System.currentTimeMillis();
            HgOwnerKey hgOwnerKey =
                    HgCliUtil.toOwnerKey(startTime + "owner:" + i, startTime + "k:" + i);
            session.put(tableName, hgOwnerKey, HgCliUtil.toBytes(i));
        }

        if (session.isTx()) {
            session.commit();
        } else {
            session.rollback();
        }

        try {
            HgKvIterator<HgKvEntry> iterable = session.scanIterator(tableName);
            int x = 0;
            while (iterable.hasNext()) {
                HgKvEntry entry = iterable.next();
                x++;
            }
            log.info("x={}", x);
        } catch (Exception e) {
            log.error("query error, message: {}", e.getMessage());
        }

        return true;
    }

    /**
     * 多线程读取文件入库
     *
     * @throws IOException
     * @throws InterruptedException
     */
    public void startMultiprocessInsert(String filepath) throws IOException {
        log.info("--- start  startMultiprocessInsert---");
        startTime = System.currentTimeMillis();
        File readfile = new File(filepath);
        MetricX metrics = null;
        long dataCount = 0;
        if (readfile.exists()) {
            // 读取文件
            InputStreamReader isr = new InputStreamReader(new FileInputStream(readfile),
                                                          StandardCharsets.UTF_8);
            BufferedReader reader = new BufferedReader(isr);

            String strLine = null;
            String tableName = HgCliUtil.TABLE_NAME;
            // 积攒到多少个后执行线程入库,10万
            int maxlist = 100000;
            List<String> keys = new ArrayList<>(maxlist);
            metrics = MetricX.ofStart();
            try {
                while ((strLine = reader.readLine()) != null) {
                    keys.add(strLine);
                    dataCount++;

                    // 读取文件中的10000条数据，启一个线程入库
                    if (dataCount % maxlist == 0) {
                        List<String> finalKeys = keys;
                        Runnable task = () -> {
                            try {
                                if (!finalKeys.isEmpty()) {
                                    boolean ret = singlePut(tableName, finalKeys);
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            taskTotal.decrementAndGet();
                            synchronized (taskTotal) {
                                taskTotal.notifyAll();
                            }
                        };
                        taskTotal.getAndIncrement();
                        threadPool.execute(task);

                        while (taskTotal.get() > 100) {
                            synchronized (taskTotal) {
                                taskTotal.wait();
                            }
                        }
                        // keys.remove(0);
                        keys = new ArrayList<>(maxlist);
                    }
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            isr.close();
            reader.close();
            // 把剩余的入库
            if (!keys.isEmpty()) {
                List<String> finalKeys1 = keys;
                Runnable task = () -> {
                    try {
                        boolean ret = singlePut(tableName, finalKeys1);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    taskTotal.decrementAndGet();
                    synchronized (taskTotal) {
                        taskTotal.notifyAll();
                    }
                };
                threadPool.execute(task);
                taskTotal.getAndIncrement();
            }
            while (taskTotal.get() > 0) {
                synchronized (taskTotal) {
                    try {
                        taskTotal.wait(1000);
                        if (taskTotal.get() > 0) {
                            System.out.println("wait thread exit " + taskTotal.get());
                        }
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }

            threadPool.shutdown();

        } else {
            System.out.println("样本文件不存在：" + filepath);
        }
        metrics.end();
        log.info("*************************************************");
        log.info("  主进程执行时间:" + metrics.past() / 1000 + "秒，一共执行：" + dataCount + "条");
        log.info("*************************************************");
        System.out.println("   主进程执行时间    " + metrics.past() / 1000 + "秒");
        System.out.println("-----主进程执行结束---------");
    }

    /**
     * 多线程读取文件入库
     *
     * @throws IOException
     * @throws InterruptedException
     */
    public void autoMultiprocessInsert() throws IOException {
        log.info("--- start  autoMultiprocessInsert---");
        startTime = System.currentTimeMillis();

        MetricX metrics = null;
        long dataCount = 0;

        String strLine = null;
        String tableName = HgCliUtil.TABLE_NAME;
        // 积攒到多少个后执行线程入库,10万
        int maxlist = 100000;
        List<String> keys = new ArrayList<>(maxlist);
        for (int x = 0; x < 10000000; x++) {
            metrics = MetricX.ofStart();
            try {
                Runnable task = () -> {
                    try {
                        boolean ret = singlePut(tableName);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    taskTotal.decrementAndGet();
                    synchronized (taskTotal) {
                        taskTotal.notifyAll();
                    }
                };
                taskTotal.getAndIncrement();
                threadPool.execute(task);

                while (taskTotal.get() > 100) {
                    synchronized (taskTotal) {
                        taskTotal.wait();
                    }
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        while (taskTotal.get() > 0) {
            synchronized (taskTotal) {
                try {
                    taskTotal.wait(1000);
                    if (taskTotal.get() > 0) {
                        System.out.println("wait thread exit " + taskTotal.get());
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        threadPool.shutdown();

        metrics.end();
        log.info("*************************************************");
        log.info("  主进程执行时间:" + metrics.past() / 1000 + "秒，一共执行：" + dataCount + "条");
        log.info("*************************************************");
        System.out.println("   主进程执行时间    " + metrics.past() / 1000 + "秒");
        System.out.println("-----主进程执行结束---------");
    }

    public String getLong() {
        //如果需要更长 或者更大冗余空间, 只需要 time * 10^n   即可
        //当前可保证1毫秒 生成 10000条不重复
        return String.format("%019x", longId.getAndIncrement());
    }

    /**
     * 执行查询，并将查询的结果做为下一次迭代的点放入队列
     */
    private void queryAnd2Queue() {
        try {
            HgStoreSession session = storeClient.openSession(graphName);
            HashSet<String> hashSet = new HashSet<>();
            while (!listQueue.isEmpty()) {

                log.info(" ====== start scanBatch2 count:{} list:{}=============",
                         queryThreadPool.getActiveCount(), listQueue.size());
                List<HgOwnerKey> keys = (List<HgOwnerKey>) listQueue.take();
                List<HgOwnerKey> newQueryList = new ArrayList<>();

                KvCloseableIterator<HgKvIterator<HgKvEntry>> iterators =
                        session.scanBatch2(
                                HgScanQuery.prefixIteratorOf(HgCliUtil.TABLE_NAME, keys.iterator())
                        );

                while (iterators.hasNext()) {
                    HgKvIterator<HgKvEntry> iterator = iterators.next();
                    int insertQueueCount = 0;
                    while (iterator.hasNext()) {
                        HgKvEntry entry = iterator.next();
                        String newPoint = HgCliUtil.toStr(entry.value());
//                        log.info("query_key =" + newPoint);
                        // 统计查询次数
                        if (!newPoint.isEmpty() && hashSet.add(newPoint)) {
                            queryCount.getAndIncrement();
                            totalQueryCount.getAndIncrement();

                            HgOwnerKey hgKey = HgCliUtil.toOwnerKey(newPoint, newPoint);
                            newQueryList.add(hgKey);

                            if (queryCount.get() > 1000000) {
                                synchronized (queryCount) {
                                    long count = queryCount.get();
                                    queryCount.set(0);
                                    if (count > 1000000) {
                                        log.info("count : " + count + " qps : " + count * 1000 /
                                                                                  (System.currentTimeMillis() -
                                                                                   startTime)
                                                 + " threadCount : " +
                                                 queryThreadPool.getActiveCount() + " queueSize:"
                                                 + listQueue.size());
                                        startTime = System.currentTimeMillis();
                                    }
                                }
                            }
                            // 达到1万个点后，去查询一次
                            if (newQueryList.size() > 10000 && listQueue.size() < 10000) {
                                listQueue.put(newQueryList);
                                insertQueueCount++;
                                newQueryList = new ArrayList<>();
                                if (insertQueueCount > 2) {
                                    break;
                                }
                            }
                        }
                    }
                }
                // 一次查询如果不够1万，单独提交一次查询，确保所有的结果都能执行查询
                if (!newQueryList.isEmpty() && listQueue.size() < 1000) {
                    listQueue.put(newQueryList);
                }

                iterators.close();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        log.info("============= thread done ==============");
        countDownLatch.countDown();
    }

    /**
     * 多线程查询
     *
     * @param point     起始查询点，后续根据这个点查询到的value做为下一次的查询条件进行迭代
     * @param scanCount 允许启动的线程数量
     * @throws IOException
     * @throws InterruptedException
     */
    public void startMultiprocessQuery(String point, String scanCount) throws IOException,
                                                                              InterruptedException {
        log.info("--- start  startMultiprocessQuery---");
        startTime = System.currentTimeMillis();
        MetricX metrics = MetricX.ofStart();
        limitScanBatchCount = Integer.parseInt(scanCount);

        CountDownLatch latch = new CountDownLatch(limitScanBatchCount);
        HgStoreSession session = storeClient.openSession(graphName);

        final AtomicLong[] counter = {new AtomicLong()};
        final long[] start = {System.currentTimeMillis()};

        LinkedBlockingQueue[] queue = new LinkedBlockingQueue[limitScanBatchCount];
        for (int i = 0; i < limitScanBatchCount; i++) {
            queue[i] = new LinkedBlockingQueue();
        }
        List<String> strKey = Arrays.asList(
                "20727483", "50329304", "26199460", "1177521", "27960125",
                "30440025", "15833920", "15015183", "33153097", "21250581");
        strKey.forEach(key -> {
            log.info("newkey:{}", key);
            HgOwnerKey hgKey = HgCliUtil.toOwnerKey(key, key);
            queue[0].add(hgKey);
        });

        for (int i = 0; i < limitScanBatchCount; i++) {
            int finalI = i;
            KvCloseableIterator<HgKvIterator<HgKvEntry>> iterators =
                    session.scanBatch2(
                            HgScanQuery.prefixIteratorOf(HgCliUtil.TABLE_NAME,
                                                         new Iterator<HgOwnerKey>() {
                                                             HgOwnerKey current = null;

                                                             @Override
                                                             public boolean hasNext() {
                                                                 while (current == null) {
                                                                     try {
                                                                         current =
                                                                                 (HgOwnerKey) queue[finalI].poll(
                                                                                         1,
                                                                                         TimeUnit.SECONDS);
                                                                     } catch (
                                                                             InterruptedException e) {
                                                                         //
                                                                     }
                                                                 }
                                                                 if (current == null) {
                                                                     log.warn(
                                                                             "===== current is " +
                                                                             "null ==========");
                                                                 }
                                                                 return current != null;
                                                             }

                                                             @Override
                                                             public HgOwnerKey next() {
                                                                 return current;
                                                             }
                                                         })
                    );

            new Thread(() -> {
                while (iterators.hasNext()) {
                    HgKvIterator<HgKvEntry> iterator = iterators.next();
                    long c = 0;
                    while (iterator.hasNext()) {
                        String newPoint = HgCliUtil.toStr(iterator.next().value());
                        HgOwnerKey newHgKey = HgCliUtil.toOwnerKey(newPoint, newPoint);
                        if (queue[(int) (c % limitScanBatchCount)].size() < 1000000) {
                            queue[(int) (c % limitScanBatchCount)].add(newHgKey);
                        }
                        c++;
                    }
                    if (counter[0].addAndGet(c) > 1000000) {
                        synchronized (counter) {
                            if (counter[0].get() > 10000000) {
                                log.info("count {}, qps {}", counter[0].get(),
                                         counter[0].get() * 1000 /
                                         (System.currentTimeMillis() - start[0]));
                                start[0] = System.currentTimeMillis();
                                counter[0].set(0);
                            }
                        }
                    }
                }
            }, "client query thread:" + i).start();
            log.info("===== read thread exit ==========");
        }
        latch.await();

        metrics.end();
        log.info("*************************************************");
        log.info("  主进程执行时间:" + metrics.past() / 1000 + "秒; 查询：" + totalQueryCount.get()
                 + "次，qps:" + totalQueryCount.get() * 1000 / metrics.past());
        log.info("*************************************************");
        System.out.println("-----主进程执行结束---------");
    }

}
