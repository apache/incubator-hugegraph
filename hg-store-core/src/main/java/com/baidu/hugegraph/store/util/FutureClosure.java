package com.baidu.hugegraph.store.util;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.error.RaftError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class FutureClosure implements Closure, Future<Status> {
    private static final Logger LOG = LoggerFactory.getLogger(FutureClosure.class);

    private CountDownLatch latch;
    private Status status;

    public FutureClosure() {
        this(1);
    }

    public FutureClosure(int count) {
        this.latch = new CountDownLatch(count);
    }

    @Override
    public void run(Status status) {
        this.status = status;
        latch.countDown();
    }

    public static void waitLatch(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            LOG.error("{}", e);
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return true;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return false;
    }

    @Override
    public Status get(){
        try {
            latch.await();
        } catch (InterruptedException e) {
            status = new Status(RaftError.EINTR, e.getMessage());
        }
        return status;
    }

    @Override
    public Status get(long timeout, TimeUnit unit){
        try {
            latch.await(timeout, unit);
        } catch (InterruptedException e) {
            status = new Status(RaftError.EINTR, e.getMessage());
        }
        return status;
    }
}
