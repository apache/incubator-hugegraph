package com.baidu.hugegraph.store.cli.util;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author lynn.bond@hotmail.com on 2022/1/29
 */
public class HgMetricX {
    private long start;
    private long end;

    private long waitStart = System.currentTimeMillis();
    private long waitTotal;

    public static HgMetricX ofStart() {
        return new HgMetricX(System.currentTimeMillis());
    }

    private HgMetricX(long start) {
        this.start = start;
    }

    ;

    public long start() {
        return this.start = System.currentTimeMillis();
    }

    public long end() {
        return this.end = System.currentTimeMillis();
    }

    public long past() {
        return this.end - this.start;
    }

    public long getWaitTotal() {
        return this.waitTotal;
    }

    public void startWait() {
        this.waitStart = System.currentTimeMillis();
    }

    public void appendWait() {
        this.waitTotal += System.currentTimeMillis() - waitStart;
    }
}
