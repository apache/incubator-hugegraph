package com.baidu.hugegraph.store.client.util;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author lynn.bond@hotmail.com on 2022/1/29
 */
public class MetricX {
    public static AtomicLong iteratorSum=new AtomicLong();
    public static AtomicLong iteratorCount=new AtomicLong();
    public static AtomicLong iteratorMax=new AtomicLong();

    public AtomicLong failureCount=new AtomicLong();

    private long start;
    private long end;

    public static MetricX ofStart(){
        return new MetricX(System.currentTimeMillis());
    }

    private MetricX(long start){
        this.start=start;
    };

    public long start(){
        return this.start=System.currentTimeMillis();
    }

    public long end(){
        return this.end=System.currentTimeMillis();
    }

    public long past(){
        return this.end-this.start;
    }

    public static void plusIteratorWait(long nanoSeconds){
        iteratorSum.addAndGet(nanoSeconds);
        iteratorCount.getAndIncrement();
        if(iteratorMax.get()<nanoSeconds){
            iteratorMax.set(nanoSeconds);
        }
    }

    /**
     * amount of waiting
     * @return millisecond
     */
    public static long getIteratorWait(){
        return iteratorSum.get()/1_000_000;
    }

    /**
     * average of waiting
     * @return millisecond
     */
    public static long getIteratorWaitAvg(){
        if(iteratorCount.get()==0)return -1;
        return getIteratorWait()/iteratorCount.get();
    }

    /**
     * maximum of waiting
     * @return millisecond
     */
    public static long getIteratorWaitMax(){
        return iteratorMax.get()/1_000_000;
    }

    public static long getIteratorCount(){
        return iteratorCount.get();
    }

    public void countFail(){
        this.failureCount.getAndIncrement();
    }

    public long getFailureCount(){
        return this.failureCount.get();
    }

}
