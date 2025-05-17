package org.apache.hugegraph.pd.client.support;

import org.apache.hugegraph.pd.common.HgAssert;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author lynn.bond@hotmail.com on 2023/12/6
 */
@Slf4j
public final class PDExecutors {
    private static final String POOL_PREFIX_NAME = "hg-pd-c-";
    private static final RejectedExecutionHandler CALLER_RUNS = new ThreadPoolExecutor.CallerRunsPolicy();
    private static final RejectedExecutionHandler ABORT_HANDLER = new ThreadPoolExecutor.AbortPolicy();
    private static final RejectedExecutionHandler DISCARD_HANDLER = new ThreadPoolExecutor.DiscardPolicy();
    private static final ExecutorService COMMON_POOL = newCachePool("common", 10, 1024);

    private PDExecutors() {
    }

    /**
     * Set up a thread pool to handle tasks fetched from an unlimited(Integer.MAX_VALUE) queue.
     * If the queue is full, the task will be run by caller.
     *
     * @param poolName   The name of the thread pool.
     * @param maxThreads The maximum number of threads in the pool.
     * @return
     */
    public static ExecutorService newQueuingPool(String poolName, int maxThreads) {
        return newQueuingPool(poolName, maxThreads, Integer.MAX_VALUE);
    }

    /**
     * Set up a thread pool to handle tasks fetched from a specific size queue.
     * If the queue is full, the task will be run by caller.
     *
     * @param poolName   The name of the thread pool.
     * @param maxThreads The maximum number of threads in the pool.
     * @param queueSize  The maximum number of tasks in the queue.
     * @return
     */
    public static ExecutorService newQueuingPool(String poolName, int maxThreads, int queueSize) {
        HgAssert.isArgumentValid(poolName, "poolName");
        HgAssert.isFalse(maxThreads <= 0, "The number of maxThreads must be positive");
        HgAssert.isFalse(queueSize <= 0, "The number of queueSize must be positive");

        return createExecutor(poolName, 0, maxThreads, queueSize, CALLER_RUNS);
    }

    /**
     * Set up a thread pool to handle tasks without queuing. The pool size increases when all threads are busy.
     * If the pool size reaches maxThreads, the task will be run by caller.
     *
     * @param poolName    The name of the thread pool.
     * @param coreThreads The size of the core threads, which will not be destroyed.
     * @param maxThreads  The maximum number of threads in the pool.
     * @return
     */
    public static ExecutorService newCachePool(String poolName, int coreThreads, int maxThreads) {
        HgAssert.isArgumentValid(poolName, "poolName");
        HgAssert.isFalse(coreThreads < 0, "The number of coreThreads must be positive or zero");
        HgAssert.isFalse(maxThreads <= 0, "The number of maxThreads must be positive");

        return createExecutor(poolName, coreThreads, maxThreads, 0, CALLER_RUNS);
    }

    /**
     * Set up a thread pool with a fixed size holding all the threads throughout whole life cycle
     * without destroying any of them.
     * All tasks submitted append to an unlimited(Integer.MAX_VALUE) queue firstly.
     * If the queue is full, the task will be aborted.
     *
     * @param poolName    The name of the thread pool.
     * @param coreThreads The number of core threads in the pool.
     * @return
     */
    public static ExecutorService newFixedPool(String poolName, int coreThreads) {
        HgAssert.isArgumentValid(poolName, "poolName");
        HgAssert.isFalse(coreThreads <= 0, "The number of threads must be positive");

        return createExecutor(poolName, coreThreads, coreThreads, Integer.MAX_VALUE, ABORT_HANDLER);
    }

    /**
     * Create a thread pool with the specified name and discarding the task if the queue is full.
     *
     * @param poolName   The name of the thread pool.
     * @param maxThreads The maximum number of threads in the pool.
     * @param queueSize  The maximum number of tasks in the queue.
     * @return
     */
    public static ExecutorService newDiscardPool(String poolName, int maxThreads, int queueSize) {
        return newDiscardPool(poolName, 0, maxThreads, queueSize);
    }

    /**
     * Create a thread pool with the specified name and discarding the task if the queue is full.
     *
     * @param poolName
     * @param coreThreads The size of the core threads, which will not be destroyed.
     * @param maxThreads  The maximum number of threads in the pool.
     * @param queueSize   The maximum number of tasks in the queue.
     * @return
     */
    public static ExecutorService newDiscardPool(String poolName, int coreThreads, int maxThreads, int queueSize) {
        HgAssert.isArgumentValid(poolName, "poolName");
        HgAssert.isFalse(coreThreads < 0, "The number of threads positive or zero");
        HgAssert.isFalse(maxThreads <= 0, "The number of threads must be positive");
        HgAssert.isFalse(queueSize < 0, "The queue size must be positive or zero");

        return createExecutor(poolName, coreThreads, maxThreads, queueSize, DISCARD_HANDLER);
    }

    /**
     * Invoking the `supplier` asynchronously and invoking the `handler` when it is completed.
     *
     * @param supplier A supplier of a synchronous procedure that returns a result.
     * @param handler  A consumer of the synchronous result.
     * @param <T>
     */
    public static <T> void asyncCallback(Supplier<T> supplier, Consumer<T> handler) {
        HgAssert.isArgumentNotNull(supplier, "supplier");
        HgAssert.isArgumentNotNull(handler, "handler");

        COMMON_POOL.execute(() -> {
            handler.accept(supplier.get());
        });
    }

    /**
     * Executing a task using a new thread.
     *
     * @param task
     * @param taskName
     */
    public static void newThreadTask(Runnable task, String taskName) {
        HgAssert.isArgumentNotNull(task, "task");
        HgAssert.isArgumentValid(taskName, "task name");
        newTaskThread(taskName, task).start();
    }

    /**
     * Attempting to execute a task without a result using the common pool.
     *
     * @param task
     * @param taskName
     * @param timeoutSec
     */
    public static void awaitTask(Runnable task, String taskName, long timeoutSec) {
        awaitTask(() -> {
            task.run();
            return true;
        }, taskName, timeoutSec);
    }

    public static void awaitTask(ExecutorService executor, Runnable task, String taskName, long timeoutSec) {
        awaitTask(executor, () -> {
            task.run();
            return true;
        }, taskName, timeoutSec);
    }

    /**
     * Attempting to execute a task using the common pool with a specified timeout (in seconds).
     */
    public static <T> T awaitTask(Callable<T> task, String taskName, long timeoutSec) {
        return awaitTask(COMMON_POOL, task, taskName, timeoutSec);
    }

    /**
     * Attempting to execute a task with a specified timeout (in seconds).
     */
    public static <T> T awaitTask(ExecutorService executor, Callable<T> task, String taskName, long timeoutSec) {
        HgAssert.isArgumentNotNull(executor, "executor");
        HgAssert.isArgumentNotNull(task, "task");
        HgAssert.isArgumentValid(taskName, "task name");
        HgAssert.isFalse(timeoutSec < 0, "The timeout must be positive");

        Future<T> future = executor.submit(task);
        T result = null;

        try {
            result = future.get(timeoutSec, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.warn("Task [ {} ] interrupted. error:", taskName, e);
        } catch (ExecutionException e) {
            log.error("Task [ {} ] execution failed. Caused by: ", taskName, e);
        } catch (TimeoutException e) {
            log.warn("Task [ {} ] did not complete within the specified timeout: [ {} ] seconds.", taskName, timeoutSec);
            future.cancel(true);
        }

        return result;
    }

    private <T> T catchError(Supplier<T> task) {
        try {
            return task.get();
        } catch (Exception e) {
            log.warn("Caught a failure of a task, error message: {}", e.getMessage());
        }

        return null;
    }

    private static ThreadPoolExecutor createExecutor(String name, int coreThreads, int maxThreads,
                                                     int queueSize, RejectedExecutionHandler handler) {
        BlockingQueue queue;

        if (queueSize <= 0) {
            queue = new SynchronousQueue();
        } else {
            queue = new LinkedBlockingQueue<>(queueSize);
        }

        return new ThreadPoolExecutor(coreThreads, maxThreads,
                60L, TimeUnit.SECONDS,
                queue, newFactory(name), handler);
    }

    private static Thread newTaskThread(String taskName, Runnable task) {
        Thread thread = new Thread(task, POOL_PREFIX_NAME + taskName.toLowerCase());
        thread.setDaemon(true);
        return thread;
    }

    private static ThreadFactory newFactory(String poolName) {
        return new ThreadFactoryBuilder().setDaemon(true).setNameFormat(getPoolName(poolName)).build();
    }

    private static String getPoolName(String name) {
        return POOL_PREFIX_NAME + name.toLowerCase() + "-%d";
    }
}
