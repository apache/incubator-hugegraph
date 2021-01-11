package com.baidu.hugegraph.unit.concurrent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.baidu.hugegraph.concurrent.BarrierEvent;
import com.baidu.hugegraph.testutil.Assert;

public class BarrierEventTest {
    
    private static int WAIT_THREADS_COUNT = 10;

    @Test(timeout = 5000)
    public void testAWait() throws InterruptedException {
        BarrierEvent barrierEvent = new BarrierEvent();
        AtomicInteger result = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(2);
        Thread awaitThread = new Thread(() -> {
            try {
                barrierEvent.await();
                result.incrementAndGet();
            } catch (InterruptedException e) {
                // Do nothing.
            } finally {
                latch.countDown();
            }
        });
        awaitThread.start();
        Thread signalThread = new Thread(() -> {
            barrierEvent.signalAll();
            latch.countDown();
        });
        signalThread.start();
        latch.await();
        Assert.assertEquals(1, result.get());
    }

    @Test
    public void testAWaitWithTimeout() throws InterruptedException {
        BarrierEvent barrierEvent = new BarrierEvent();
        boolean signaled = barrierEvent.await(1L);
        Assert.assertFalse(signaled);
    }

    @Test
    public void testReset() throws InterruptedException {
        BarrierEvent barrierEvent = new BarrierEvent();
        boolean signaled = barrierEvent.await(1L);
        Assert.assertFalse(signaled);
        barrierEvent.signal();
        signaled = barrierEvent.await(1L);
        Assert.assertTrue(signaled);
        barrierEvent.reset();
        signaled = barrierEvent.await(1L);
        Assert.assertFalse(signaled);
    }

    @Test
    public void testSignal() throws InterruptedException {
        BarrierEvent barrierEvent = new BarrierEvent();
        boolean signaled = barrierEvent.await(1L);
        Assert.assertFalse(signaled);
        barrierEvent.signal();
        signaled = barrierEvent.await(1L);
        Assert.assertTrue(signaled);
    }

    @Test
    public void testSignalByMultiThreadWithSignalFirst()
                throws InterruptedException {
        BarrierEvent barrierEvent = new BarrierEvent();
        AtomicInteger eventCount = new AtomicInteger(0);
        AtomicInteger waitThreadInterruptedCount = new AtomicInteger(0);
        ExecutorService executorService =
                        Executors.newFixedThreadPool(WAIT_THREADS_COUNT + 1);
        CountDownLatch waitLatch = new CountDownLatch(WAIT_THREADS_COUNT);
        CountDownLatch signalLatch = new CountDownLatch(1);
        for (int i = 0; i < WAIT_THREADS_COUNT; i++) {
            executorService.submit(() -> {
                try {
                    signalLatch.await();
                    barrierEvent.await();
                    eventCount.incrementAndGet();
                } catch (InterruptedException e) {
                    waitThreadInterruptedCount.incrementAndGet();
                } finally {
                    waitLatch.countDown();
                }
            });
        }

        executorService.submit(() -> {
            barrierEvent.signal();
            signalLatch.countDown();
        });

        executorService.shutdown();
        executorService.awaitTermination(2L, TimeUnit.SECONDS);
        waitLatch.await();
        Assert.assertEquals(10, eventCount.get());
        Assert.assertEquals(0, waitThreadInterruptedCount.get());
    }

    @Test
    public void testSignalByMultiThreadWithSignalLast()
                throws InterruptedException {
        BarrierEvent barrierEvent = new BarrierEvent();
        AtomicInteger eventCount = new AtomicInteger(0);
        AtomicInteger waitThreadInterruptedCount = new AtomicInteger(0);
        AtomicInteger signalThreadInterruptedCount = new AtomicInteger(0);
        ExecutorService executorService =
                        Executors.newFixedThreadPool(WAIT_THREADS_COUNT + 1);
        CountDownLatch waitLatch = new CountDownLatch(WAIT_THREADS_COUNT);
        CountDownLatch signalLatch = new CountDownLatch(1);
        for (int i = 0; i < WAIT_THREADS_COUNT; i++) {
            executorService.submit(() -> {
                try {
                    waitLatch.countDown();
                    barrierEvent.await();
                    eventCount.incrementAndGet();
                } catch (InterruptedException e) {
                    waitThreadInterruptedCount.incrementAndGet();
                }
            });
        }

        executorService.submit(() -> {
            try {
                waitLatch.await();
            } catch (InterruptedException e) {
                signalThreadInterruptedCount.incrementAndGet();
            }
            barrierEvent.signal();
            signalLatch.countDown();
        });
        signalLatch.await();
        executorService.shutdownNow();
        executorService.awaitTermination(1L, TimeUnit.SECONDS);
        Assert.assertEquals(1, eventCount.get());
        Assert.assertEquals(WAIT_THREADS_COUNT - 1,
                            waitThreadInterruptedCount.get());
        Assert.assertEquals(0, signalThreadInterruptedCount.get());
    }

    @Test
    public void testSignalAll() throws InterruptedException {
        BarrierEvent barrierEvent = new BarrierEvent();
        boolean signaled = barrierEvent.await(1L);
        Assert.assertFalse(signaled);
        barrierEvent.signalAll();
        signaled = barrierEvent.await(1L);
        Assert.assertTrue(signaled);
    }

    @Test
    public void testSignalAllByMultiThreadWithSignalFirst()
                throws InterruptedException {
        BarrierEvent barrierEvent = new BarrierEvent();
        AtomicInteger eventCount = new AtomicInteger(0);
        AtomicInteger waitThreadInterruptedCount = new AtomicInteger(0);
        ExecutorService executorService =
                        Executors.newFixedThreadPool(WAIT_THREADS_COUNT + 1);
        CountDownLatch waitLatch = new CountDownLatch(WAIT_THREADS_COUNT);
        CountDownLatch signalLatch = new CountDownLatch(1);
        for (int i = 0; i < WAIT_THREADS_COUNT; i++) {
            executorService.submit(() -> {
                try {
                    signalLatch.await();
                    waitLatch.countDown();
                    barrierEvent.await();
                    eventCount.incrementAndGet();
                } catch (InterruptedException e) {
                    waitThreadInterruptedCount.incrementAndGet();
                }
            });
        }

        executorService.submit(() -> {
            barrierEvent.signalAll();
            signalLatch.countDown();
        });

        executorService.shutdown();
        executorService.awaitTermination(1L, TimeUnit.SECONDS);
        Assert.assertEquals(10, eventCount.get());
        Assert.assertEquals(0, waitThreadInterruptedCount.get());
    }

    @Test
    public void testSignalAllByMultiThreadWithSignalLast()
                throws InterruptedException {
        BarrierEvent barrierEvent = new BarrierEvent();
        AtomicInteger eventCount = new AtomicInteger(0);
        AtomicInteger waitThreadInterruptedCount = new AtomicInteger(0);
        AtomicInteger signalThreadInterruptedCount = new AtomicInteger(0);
        ExecutorService executorService =
                        Executors.newFixedThreadPool(WAIT_THREADS_COUNT + 1);
        CountDownLatch waitLatch = new CountDownLatch(WAIT_THREADS_COUNT);
        CountDownLatch signalLatch = new CountDownLatch(1);
        for (int i = 0; i < WAIT_THREADS_COUNT; i++) {
            executorService.submit(() -> {
                try {
                    waitLatch.countDown();
                    barrierEvent.await();
                    eventCount.incrementAndGet();
                } catch (InterruptedException e) {
                    waitThreadInterruptedCount.incrementAndGet();
                }
            });
        }

        executorService.submit(() -> {
            try {
                waitLatch.await();
            } catch (InterruptedException e) {
                signalThreadInterruptedCount.incrementAndGet();
            }
            barrierEvent.signalAll();
            signalLatch.countDown();
        });
        signalLatch.await();
        executorService.shutdown();
        executorService.awaitTermination(1L, TimeUnit.SECONDS);
        Assert.assertEquals(WAIT_THREADS_COUNT, eventCount.get());
        Assert.assertEquals(0, waitThreadInterruptedCount.get());
        Assert.assertEquals(0, signalThreadInterruptedCount.get());
    }

    @Test
    public void testSignalAllByMultiThreadWithSignalAwaitConcurrent()
                throws InterruptedException {
        BarrierEvent barrierEvent = new BarrierEvent();
        AtomicInteger eventCount = new AtomicInteger(0);
        AtomicInteger waitThreadInterruptedCount = new AtomicInteger(0);
        AtomicInteger signalThreadInterruptedCount = new AtomicInteger(0);
        ExecutorService executorService =
                        Executors.newFixedThreadPool(WAIT_THREADS_COUNT + 1);
        CountDownLatch syncLatch = new CountDownLatch(1);
        for (int i = 0; i < WAIT_THREADS_COUNT; i++) {
            executorService.submit(() -> {
                try {
                    syncLatch.await();
                    barrierEvent.await();
                    eventCount.incrementAndGet();
                } catch (InterruptedException e) {
                    waitThreadInterruptedCount.incrementAndGet();
                }
            });
        }

        executorService.submit(() -> {
            try {
                syncLatch.await();
            } catch (InterruptedException e) {
                signalThreadInterruptedCount.incrementAndGet();
            }
            barrierEvent.signalAll();
        });
        syncLatch.countDown();
        executorService.shutdown();
        executorService.awaitTermination(1L, TimeUnit.SECONDS);
        Assert.assertEquals(WAIT_THREADS_COUNT, eventCount.get());
        Assert.assertEquals(0, waitThreadInterruptedCount.get());
        Assert.assertEquals(0, signalThreadInterruptedCount.get());
    }
}
