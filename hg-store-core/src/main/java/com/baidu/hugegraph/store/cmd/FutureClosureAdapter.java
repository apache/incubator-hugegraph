package com.baidu.hugegraph.store.cmd;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;

import java.util.concurrent.CompletableFuture;

public class FutureClosureAdapter<T> implements Closure {
    public final CompletableFuture<T> future = new CompletableFuture<>();
    private T resp;

    public T getResponse() { return this.resp; }

    public void setResponse(T resp) {
        this.resp = resp;
        future.complete(resp);
        run(Status.OK());
    }

    public void failure(Throwable t){
        future.completeExceptionally(t);
        run(new Status(-1, t.getMessage()));
    }

    @Override
    public void run(Status status) {

    }
}
